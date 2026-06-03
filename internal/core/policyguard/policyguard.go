package policyguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	ReasonCodePolicyViolation  = "policy_violation"
	AuthoritySourcePolicyGuard = "policy_guard"
)

type DecisionLogger interface {
	LogPolicyDecision(ctx context.Context, decision Decision) error
}

type Decision struct {
	Allowed         bool
	AuthoritySource string
	ReasonCode      string
	ReasonText      string
	RoleID          string
	ActionType      string
	ToolClass       string
	Command         string
	CommandClass    string
	Path            string
	Domain          string
	ForbiddenClass  string
}

type Action struct {
	RoleID         string
	NodeClass      string
	ActionType     string
	ToolClasses    []string
	Command        string
	CommandClasses []string
	WritePaths     []string
	Domains        []string
}

type PolicyViolationError struct {
	ForbiddenClass string
	Detail         string
}

func (e *PolicyViolationError) Error() string {
	if e.ForbiddenClass != "" {
		return fmt.Sprintf("%s: %s (%s)", ReasonCodePolicyViolation, e.Detail, e.ForbiddenClass)
	}
	return fmt.Sprintf("%s: %s", ReasonCodePolicyViolation, e.Detail)
}

type Guard struct {
	rolePolicy    RolePolicy
	networkPolicy NetworkPolicy
	logger        DecisionLogger
}

func LoadFromFiles(rolePolicyPath, networkPolicyPath string, logger DecisionLogger) (*Guard, error) {
	rolePolicy, err := loadRolePolicy(rolePolicyPath)
	if err != nil {
		return nil, err
	}

	networkPolicy, err := loadNetworkPolicy(networkPolicyPath)
	if err != nil {
		return nil, err
	}

	return NewGuard(rolePolicy, networkPolicy, logger)
}

func NewGuard(rolePolicy RolePolicy, networkPolicy NetworkPolicy, logger DecisionLogger) (*Guard, error) {
	if rolePolicy.Mode != "deny_by_default" {
		return nil, fmt.Errorf("role policy mode %q is unsupported", rolePolicy.Mode)
	}
	if networkPolicy.Mode != "deny_by_default" {
		return nil, fmt.Errorf("network policy mode %q is unsupported", networkPolicy.Mode)
	}

	if len(rolePolicy.ForbiddenCommandClasses) == 0 {
		return nil, errors.New("role policy must define forbidden command classes")
	}

	return &Guard{rolePolicy: rolePolicy, networkPolicy: networkPolicy, logger: logger}, nil
}

func (g *Guard) Evaluate(ctx context.Context, action Action) error {
	if action.RoleID == "" {
		return errors.New("role id is required")
	}

	roleRules, ok := g.rolePolicy.Roles[action.RoleID]
	if !ok {
		return g.deny(ctx, action, "role is not present in policy", "", "", "", "")
	}

	for _, toolClass := range action.ToolClasses {
		if !contains(roleRules.AllowedToolClasses, toolClass) {
			return g.deny(ctx, action, fmt.Sprintf("tool class %q is not allowed for role %q", toolClass, action.RoleID), "", toolClass, "", "")
		}
	}

	forbiddenClass, forbidden := classifyForbiddenCommand(action.Command)
	if forbidden {
		return g.deny(ctx, action, fmt.Sprintf("command %q matches forbidden command class %q", action.Command, forbiddenClass), forbiddenClass, "", action.Command, "")
	}

	commandClasses := append([]string(nil), action.CommandClasses...)
	if len(commandClasses) == 0 && strings.TrimSpace(action.Command) != "" {
		if inferred, ok := inferCommandClass(action.Command); ok {
			commandClasses = []string{inferred}
		}
	}

	for _, commandClass := range commandClasses {
		if !contains(roleRules.AllowedCommandClasses, commandClass) {
			return g.deny(ctx, action, fmt.Sprintf("command class %q is not allowed for role %q", commandClass, action.RoleID), "", "", action.Command, commandClass)
		}
	}

	for _, path := range action.WritePaths {
		if err := g.checkPathAllowed(path, roleRules); err != nil {
			return g.deny(ctx, action, err.Error(), "out_of_workspace_write", "", "", "")
		}
	}

	if err := g.checkDomainsAllowed(ctx, action); err != nil {
		return err
	}

	return g.logDecision(ctx, Decision{
		Allowed:         true,
		AuthoritySource: AuthoritySourcePolicyGuard,
		ReasonCode:      "allowed",
		ReasonText:      "action approved by deny-by-default policy",
		RoleID:          action.RoleID,
		ActionType:      action.ActionType,
		Command:         action.Command,
	})
}

func (g *Guard) checkPathAllowed(targetPath string, roleRules RoleRule) error {
	cleanPath := filepath.Clean(targetPath)
	for _, scopeID := range roleRules.AllowedPathScopes {
		scopeRoot, ok := g.rolePolicy.PathScopes[scopeID]
		if !ok {
			continue
		}

		scopeRoot = filepath.Clean(scopeRoot)
		if cleanPath == scopeRoot {
			return nil
		}
		if strings.HasPrefix(cleanPath, scopeRoot+string(filepath.Separator)) {
			return nil
		}
	}

	return fmt.Errorf("write path %q is outside role scope", targetPath)
}

func (g *Guard) checkDomainsAllowed(ctx context.Context, action Action) error {
	if len(action.Domains) == 0 {
		return nil
	}

	roleRules, ok := g.networkPolicy.Roles[action.RoleID]
	if !ok {
		return g.deny(ctx, action, fmt.Sprintf("network policy missing role %q", action.RoleID), "network_unallowlisted", "", "", "")
	}

	allowed := make(map[string]struct{})
	for _, domain := range roleRules.DefaultAllowedDomains {
		allowed[strings.ToLower(domain)] = struct{}{}
	}
	if override, ok := roleRules.NodeClassOverrides[action.NodeClass]; ok {
		for _, domain := range override {
			allowed[strings.ToLower(domain)] = struct{}{}
		}
	}

	globalDeniedHosts := make(map[string]struct{}, len(g.networkPolicy.GlobalDenies.Hostnames))
	for _, hostname := range g.networkPolicy.GlobalDenies.Hostnames {
		globalDeniedHosts[strings.ToLower(hostname)] = struct{}{}
	}

	for _, requested := range action.Domains {
		normalized := strings.ToLower(strings.TrimSpace(requested))
		if normalized == "" {
			return g.deny(ctx, action, "network allowlist contains empty domain", "network_unallowlisted", "", "", "")
		}
		if _, denied := globalDeniedHosts[normalized]; denied {
			return g.deny(ctx, action, fmt.Sprintf("domain %q is globally denied", normalized), "network_unallowlisted", "", "", "")
		}
		if _, ok := allowed[normalized]; !ok {
			return g.deny(ctx, action, fmt.Sprintf("domain %q is not allowlisted for role %q node class %q", normalized, action.RoleID, action.NodeClass), "network_unallowlisted", "", "", "")
		}
	}

	return nil
}

func (g *Guard) deny(ctx context.Context, action Action, reason, forbiddenClass, toolClass, command, commandClass string) error {
	decision := Decision{
		Allowed:         false,
		AuthoritySource: AuthoritySourcePolicyGuard,
		ReasonCode:      ReasonCodePolicyViolation,
		ReasonText:      reason,
		RoleID:          action.RoleID,
		ActionType:      action.ActionType,
		ToolClass:       toolClass,
		Command:         command,
		CommandClass:    commandClass,
		ForbiddenClass:  forbiddenClass,
	}
	if err := g.logDecision(ctx, decision); err != nil {
		return err
	}

	return &PolicyViolationError{ForbiddenClass: forbiddenClass, Detail: reason}
}

func (g *Guard) logDecision(ctx context.Context, decision Decision) error {
	if g.logger == nil {
		return nil
	}
	if err := g.logger.LogPolicyDecision(ctx, decision); err != nil {
		return fmt.Errorf("log policy decision: %w", err)
	}
	return nil
}

func loadRolePolicy(path string) (RolePolicy, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return RolePolicy{}, fmt.Errorf("read role policy %q: %w", path, err)
	}

	var policy RolePolicy
	if err := yaml.Unmarshal(raw, &policy); err != nil {
		return RolePolicy{}, fmt.Errorf("decode role policy %q: %w", path, err)
	}
	if policy.Version == "" || policy.PolicyID == "" {
		return RolePolicy{}, fmt.Errorf("role policy %q is missing required version/policy_id", path)
	}
	if policy.PathScopes == nil {
		policy.PathScopes = map[string]string{}
	}
	if policy.Roles == nil {
		policy.Roles = map[string]RoleRule{}
	}

	policy.ForbiddenCommandClasses = canonicalize(policy.ForbiddenCommandClasses)
	for roleID, roleRule := range policy.Roles {
		roleRule.AllowedToolClasses = canonicalize(roleRule.AllowedToolClasses)
		roleRule.AllowedCommandClasses = canonicalize(roleRule.AllowedCommandClasses)
		roleRule.AllowedPathScopes = canonicalize(roleRule.AllowedPathScopes)
		policy.Roles[roleID] = roleRule
	}

	return policy, nil
}

func loadNetworkPolicy(path string) (NetworkPolicy, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return NetworkPolicy{}, fmt.Errorf("read network policy %q: %w", path, err)
	}

	var policy NetworkPolicy
	if err := yaml.Unmarshal(raw, &policy); err != nil {
		return NetworkPolicy{}, fmt.Errorf("decode network policy %q: %w", path, err)
	}
	if policy.Version == "" || policy.PolicyID == "" {
		return NetworkPolicy{}, fmt.Errorf("network policy %q is missing required version/policy_id", path)
	}
	if policy.Roles == nil {
		policy.Roles = map[string]NetworkRoleRule{}
	}

	for roleID, roleRule := range policy.Roles {
		roleRule.DefaultAllowedDomains = canonicalize(roleRule.DefaultAllowedDomains)
		if roleRule.NodeClassOverrides == nil {
			roleRule.NodeClassOverrides = map[string][]string{}
		}
		for nodeClass, domains := range roleRule.NodeClassOverrides {
			roleRule.NodeClassOverrides[nodeClass] = canonicalize(domains)
		}
		policy.Roles[roleID] = roleRule
	}

	policy.GlobalDenies.Hostnames = canonicalize(policy.GlobalDenies.Hostnames)
	policy.GlobalDenies.CIDRBlocks = canonicalize(policy.GlobalDenies.CIDRBlocks)

	return policy, nil
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func canonicalize(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized := strings.TrimSpace(strings.ToLower(value))
		if normalized == "" {
			continue
		}
		set[normalized] = struct{}{}
	}

	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
