package policyguard

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
)

func TestInferCommandClass(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		command string
		want    string
		ok      bool
	}{
		{name: "go test", command: "go test ./...", want: "test_runner", ok: true},
		{name: "go toolchain", command: "go vet ./...", want: "go_toolchain", ok: true},
		{name: "pnpm test", command: "pnpm test", want: "test_runner", ok: true},
		{name: "pnpm install", command: "pnpm install", want: "node_toolchain", ok: true},
		{name: "unknown", command: "ls -la", want: "", ok: false},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := inferCommandClass(tc.command)
			if ok != tc.ok {
				t.Fatalf("unexpected classification state: got=%t want=%t", ok, tc.ok)
			}
			if got != tc.want {
				t.Fatalf("unexpected inferred class: got=%q want=%q", got, tc.want)
			}
		})
	}
}

func TestPolicyViolationErrorMessageFormatting(t *testing.T) {
	t.Parallel()

	withClass := (&PolicyViolationError{ForbiddenClass: "git_destructive", Detail: "blocked command"}).Error()
	if withClass != "policy_violation: blocked command (git_destructive)" {
		t.Fatalf("unexpected message with class: %q", withClass)
	}

	withoutClass := (&PolicyViolationError{Detail: "missing role"}).Error()
	if withoutClass != "policy_violation: missing role" {
		t.Fatalf("unexpected message without class: %q", withoutClass)
	}
}

func TestNewGuardValidatesPolicyModesAndForbiddenClasses(t *testing.T) {
	t.Parallel()

	_, err := NewGuard(RolePolicy{Mode: "allow_by_default"}, NetworkPolicy{Mode: "deny_by_default"}, nil)
	if err == nil {
		t.Fatal("expected role policy mode validation error")
	}

	_, err = NewGuard(RolePolicy{Mode: "deny_by_default", ForbiddenCommandClasses: []string{"git_destructive"}}, NetworkPolicy{Mode: "allow_by_default"}, nil)
	if err == nil {
		t.Fatal("expected network policy mode validation error")
	}

	_, err = NewGuard(RolePolicy{Mode: "deny_by_default"}, NetworkPolicy{Mode: "deny_by_default"}, nil)
	if err == nil {
		t.Fatal("expected forbidden command classes validation error")
	}
}

func TestGuardEvaluateInfersCommandClassWhenNotProvided(t *testing.T) {
	t.Parallel()

	guard := newTestGuard(t, &memoryDecisionLogger{})
	err := guard.Evaluate(context.Background(), Action{
		RoleID:      "builder-berserker",
		NodeClass:   "build",
		ActionType:  "container_start",
		ToolClasses: []string{"workspace_write"},
		Command:     "go test ./...",
		WritePaths:  []string{"/workspace/run-a"},
		Domains:     []string{"proxy.golang.org"},
	})
	if err != nil {
		t.Fatalf("expected inferred command class to allow action: %v", err)
	}
}

func TestGuardEvaluateReturnsLoggerError(t *testing.T) {
	t.Parallel()

	guard := newTestGuard(t, failingDecisionLogger{err: errors.New("logger down")})
	err := guard.Evaluate(context.Background(), Action{
		RoleID:      "builder-berserker",
		NodeClass:   "build",
		ActionType:  "container_start",
		ToolClasses: []string{"workspace_write"},
		Command:     "go test ./...",
		WritePaths:  []string{"/workspace/run-a"},
		Domains:     []string{"proxy.golang.org"},
	})
	if err == nil {
		t.Fatal("expected logger error")
	}
	if err.Error() != "log policy decision: logger down" {
		t.Fatalf("unexpected logger error: %v", err)
	}
}

func TestGuardRoleAllowDenyMatrix(t *testing.T) {
	t.Parallel()

	logger := &memoryDecisionLogger{}
	guard := newTestGuard(t, logger)

	err := guard.Evaluate(context.Background(), Action{
		RoleID:         "builder-berserker",
		NodeClass:      "build",
		ActionType:     "container_start",
		ToolClasses:    []string{"workspace_write", "build"},
		Command:        "go test ./...",
		CommandClasses: []string{"test_runner"},
		WritePaths:     []string{"/workspace/run-a"},
		Domains:        []string{"proxy.golang.org", "sum.golang.org"},
	})
	if err != nil {
		t.Fatalf("expected builder action to be allowed: %v", err)
	}

	err = guard.Evaluate(context.Background(), Action{
		RoleID:      "king",
		NodeClass:   "decree_intake",
		ActionType:  "governance_review",
		ToolClasses: []string{"workspace_write"},
	})
	if err == nil {
		t.Fatal("expected king workspace_write to be denied")
	}

	var violation *PolicyViolationError
	if !errors.As(err, &violation) {
		t.Fatalf("expected PolicyViolationError, got %T", err)
	}
	if len(logger.decisions) != 2 {
		t.Fatalf("expected two decisions to be logged, got %d", len(logger.decisions))
	}
	if logger.decisions[0].ReasonCode != "allowed" {
		t.Fatalf("expected first decision allowed, got %q", logger.decisions[0].ReasonCode)
	}
	if logger.decisions[1].ReasonCode != ReasonCodePolicyViolation {
		t.Fatalf("expected second decision policy_violation, got %q", logger.decisions[1].ReasonCode)
	}
}

func TestGuardBlocksForbiddenCommandClasses(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		command       string
		forbiddenType string
	}{
		{name: "git_destructive", command: "git reset --hard", forbiddenType: "git_destructive"},
		{name: "privilege_escalation", command: "sudo rm -rf /workspace/tmp", forbiddenType: "privilege_escalation"},
		{name: "host_system_mutation", command: "brew install jq", forbiddenType: "host_system_mutation"},
		{name: "direct_state_bypass", command: "sqlite3 kingdom.db update workflow_runs set run_status='failed'", forbiddenType: "direct_state_bypass"},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			guard := newTestGuard(t, &memoryDecisionLogger{})
			err := guard.Evaluate(context.Background(), Action{
				RoleID:         "builder-berserker",
				NodeClass:      "build",
				ActionType:     "container_start",
				ToolClasses:    []string{"workspace_write"},
				Command:        tc.command,
				CommandClasses: []string{"git_safe"},
				WritePaths:     []string{"/workspace/run-a"},
			})
			if err == nil {
				t.Fatalf("expected %s command to be denied", tc.forbiddenType)
			}

			var violation *PolicyViolationError
			if !errors.As(err, &violation) {
				t.Fatalf("expected PolicyViolationError, got %T", err)
			}
			if violation.ForbiddenClass != tc.forbiddenType {
				t.Fatalf("expected forbidden class %q, got %q", tc.forbiddenType, violation.ForbiddenClass)
			}
		})
	}
}

func TestGuardBlocksOutOfScopeWriteAndUnallowlistedNetwork(t *testing.T) {
	t.Parallel()

	guard := newTestGuard(t, &memoryDecisionLogger{})

	err := guard.Evaluate(context.Background(), Action{
		RoleID:         "builder-berserker",
		NodeClass:      "build",
		ActionType:     "container_start",
		ToolClasses:    []string{"workspace_write"},
		Command:        "go test ./...",
		CommandClasses: []string{"test_runner"},
		WritePaths:     []string{"/etc"},
	})
	if err == nil {
		t.Fatal("expected out-of-scope path to be denied")
	}

	err = guard.Evaluate(context.Background(), Action{
		RoleID:         "builder-berserker",
		NodeClass:      "build",
		ActionType:     "container_start",
		ToolClasses:    []string{"workspace_write"},
		Command:        "go test ./...",
		CommandClasses: []string{"test_runner"},
		WritePaths:     []string{"/workspace/run-a"},
		Domains:        []string{"example.com"},
	})
	if err == nil {
		t.Fatal("expected unallowlisted domain to be denied")
	}

	var violation *PolicyViolationError
	if !errors.As(err, &violation) {
		t.Fatalf("expected PolicyViolationError, got %T", err)
	}
	if violation.ForbiddenClass != "network_unallowlisted" {
		t.Fatalf("expected network_unallowlisted, got %q", violation.ForbiddenClass)
	}
}

func TestGuardLogsAuthorityAndReasonOnAllowAndDeny(t *testing.T) {
	t.Parallel()

	logger := &memoryDecisionLogger{}
	guard := newTestGuard(t, logger)

	err := guard.Evaluate(context.Background(), Action{
		RoleID:         "builder-berserker",
		NodeClass:      "build",
		ActionType:     "container_start",
		ToolClasses:    []string{"workspace_write"},
		Command:        "go test ./...",
		CommandClasses: []string{"test_runner"},
		WritePaths:     []string{"/workspace/run-a"},
		Domains:        []string{"proxy.golang.org"},
	})
	if err != nil {
		t.Fatalf("allow action: %v", err)
	}

	err = guard.Evaluate(context.Background(), Action{
		RoleID:         "builder-berserker",
		NodeClass:      "build",
		ActionType:     "container_start",
		ToolClasses:    []string{"workspace_write"},
		Command:        "git reset --hard",
		CommandClasses: []string{"git_safe"},
		WritePaths:     []string{"/workspace/run-a"},
	})
	if err == nil {
		t.Fatal("expected deny action")
	}

	if len(logger.decisions) != 2 {
		t.Fatalf("expected two logged decisions, got %d", len(logger.decisions))
	}

	allow := logger.decisions[0]
	if !allow.Allowed || allow.AuthoritySource != AuthoritySourcePolicyGuard || allow.ReasonCode != "allowed" {
		t.Fatalf("unexpected allow decision: %+v", allow)
	}

	deny := logger.decisions[1]
	if deny.Allowed {
		t.Fatalf("expected deny decision: %+v", deny)
	}
	if deny.AuthoritySource != AuthoritySourcePolicyGuard {
		t.Fatalf("expected policy_guard authority, got %q", deny.AuthoritySource)
	}
	if deny.ReasonCode != ReasonCodePolicyViolation || deny.ReasonText == "" {
		t.Fatalf("unexpected deny reason fields: %+v", deny)
	}
}

func newTestGuard(t *testing.T, logger DecisionLogger) *Guard {
	t.Helper()
	rolePath, networkPath := policyPaths(t)

	guard, err := LoadFromFiles(rolePath, networkPath, logger)
	if err != nil {
		t.Fatalf("load policy guard: %v", err)
	}
	return guard
}

func policyPaths(t *testing.T) (string, string) {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test file path")
	}
	rootDir := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
	return filepath.Join(rootDir, "policies", "role_policy.yaml"), filepath.Join(rootDir, "policies", "network_policy.yaml")
}

type memoryDecisionLogger struct {
	decisions []Decision
}

func (l *memoryDecisionLogger) LogPolicyDecision(_ context.Context, decision Decision) error {
	l.decisions = append(l.decisions, decision)
	return nil
}

type failingDecisionLogger struct {
	err error
}

func (l failingDecisionLogger) LogPolicyDecision(_ context.Context, _ Decision) error {
	if l.err == nil {
		return fmt.Errorf("logger failure")
	}
	return l.err
}
