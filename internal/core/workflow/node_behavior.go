package workflow

import (
	"fmt"
	"strings"
)

type NodeExecutionOwnership string

const (
	NodeExecutionOwnershipProviderBacked NodeExecutionOwnership = "provider_backed"
	NodeExecutionOwnershipRuntimeOwned   NodeExecutionOwnership = "runtime_owned"
)

type NodeArtifactPersistence string

const (
	NodeArtifactPersistenceWorkspace NodeArtifactPersistence = "workspace"
	NodeArtifactPersistenceDirect    NodeArtifactPersistence = "direct"
)

// ProviderCompletionMode is runtime-owned completion authority for a canonical
// node class. It is deliberately not workflow-authored: a workflow may select
// a canonical class and role only, never weaken that class's settlement rules.
type ProviderCompletionMode string

const (
	ProviderCompletionModeNotApplicable  ProviderCompletionMode = "not_applicable"
	ProviderCompletionModeServerResponse ProviderCompletionMode = "server_response"
)

type NodeBehavior struct {
	ExecutionOwnership           NodeExecutionOwnership
	ProviderCompletionMode       ProviderCompletionMode
	ArtifactPersistence          NodeArtifactPersistence
	ProjectArtifacts             bool
	IncludeInExecutionAnalytics  bool
	WaitsForRecordedPlanDecision bool
}

type canonicalNodeContract struct {
	behavior      NodeBehavior
	requiredRoles map[string]struct{}
}

func roleSet(roles ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(roles))
	for _, role := range roles {
		set[role] = struct{}{}
	}
	return set
}

var canonicalNodeContracts = map[string]canonicalNodeContract{
	NodeClassWorkIntake: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipRuntimeOwned, ProviderCompletionMode: ProviderCompletionModeNotApplicable, ArtifactPersistence: NodeArtifactPersistenceDirect},
		requiredRoles: roleSet("governance"),
	},
	NodeClassAdvisorConsultation: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipProviderBacked, ProviderCompletionMode: ProviderCompletionModeServerResponse, ArtifactPersistence: NodeArtifactPersistenceDirect, ProjectArtifacts: true, IncludeInExecutionAnalytics: true},
		requiredRoles: roleSet("architecture-advisor", "risk-advisor", "systems-advisor", "usability-advisor"),
	},
	NodeClassAdvisorySynthesis: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipProviderBacked, ProviderCompletionMode: ProviderCompletionModeServerResponse, ArtifactPersistence: NodeArtifactPersistenceDirect, ProjectArtifacts: true, IncludeInExecutionAnalytics: true},
		requiredRoles: roleSet("coordinator"),
	},
	NodeClassPlanDrafting: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipProviderBacked, ProviderCompletionMode: ProviderCompletionModeServerResponse, ArtifactPersistence: NodeArtifactPersistenceDirect, ProjectArtifacts: true, IncludeInExecutionAnalytics: true},
		requiredRoles: roleSet("plan-author"),
	},
	NodeClassPlanDecision: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipRuntimeOwned, ProviderCompletionMode: ProviderCompletionModeNotApplicable, ArtifactPersistence: NodeArtifactPersistenceDirect, WaitsForRecordedPlanDecision: true},
		requiredRoles: roleSet("governance"),
	},
	NodeClassExecutionKickoff: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipRuntimeOwned, ProviderCompletionMode: ProviderCompletionModeNotApplicable, ArtifactPersistence: NodeArtifactPersistenceDirect},
		requiredRoles: roleSet("plan-author"),
	},
	NodeClassDelegatedExecution: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipProviderBacked, ProviderCompletionMode: ProviderCompletionModeServerResponse, ArtifactPersistence: NodeArtifactPersistenceWorkspace, ProjectArtifacts: true, IncludeInExecutionAnalytics: true},
		requiredRoles: roleSet("builder"),
	},
	NodeClassExecutionReview: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipProviderBacked, ProviderCompletionMode: ProviderCompletionModeServerResponse, ArtifactPersistence: NodeArtifactPersistenceDirect, ProjectArtifacts: true, IncludeInExecutionAnalytics: true},
		requiredRoles: roleSet("reviewer"),
	},
	NodeClassValidationGates: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipProviderBacked, ProviderCompletionMode: ProviderCompletionModeServerResponse, ArtifactPersistence: NodeArtifactPersistenceDirect, ProjectArtifacts: true, IncludeInExecutionAnalytics: true},
		requiredRoles: roleSet("qa-validator", "security-validator", "resilience-validator"),
	},
	NodeClassCompletion: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipRuntimeOwned, ProviderCompletionMode: ProviderCompletionModeNotApplicable, ArtifactPersistence: NodeArtifactPersistenceDirect},
		requiredRoles: roleSet("governance"),
	},
}

func CanonicalNodeClassForNodeID(nodeID string) string {
	value := strings.TrimSpace(strings.ToLower(nodeID))
	// Built-in node names describe their purpose; execution classes describe
	// their authority and settlement contract. Keep that mapping canonical.
	switch value {
	case "architecture_consultation", "risk_consultation", "systems_consultation", "usability_consultation":
		return NodeClassAdvisorConsultation
	case "implementation":
		return NodeClassDelegatedExecution
	case "quality_validation", "security_validation", "resilience_validation":
		return NodeClassValidationGates
	}
	for _, classID := range []string{
		NodeClassWorkIntake,
		NodeClassAdvisorConsultation,
		NodeClassAdvisorySynthesis,
		NodeClassPlanDrafting,
		NodeClassPlanDecision,
		NodeClassExecutionKickoff,
		NodeClassDelegatedExecution,
		NodeClassExecutionReview,
		NodeClassValidationGates,
		NodeClassCompletion,
	} {
		if value == classID || strings.HasPrefix(value, classID+"_") {
			return classID
		}
	}
	return value
}

func ResolveNodeClass(nodeID, nodeClass string) string {
	trimmedClass := strings.TrimSpace(nodeClass)
	if trimmedClass != "" {
		return trimmedClass
	}
	return CanonicalNodeClassForNodeID(nodeID)
}

func BehaviorForNode(nodeID, nodeClass string) NodeBehavior {
	contract, ok := canonicalNodeContracts[ResolveNodeClass(nodeID, nodeClass)]
	if !ok {
		return NodeBehavior{}
	}
	return contract.behavior
}

// BehaviorForNodeContract resolves the canonical behavior and validates that
// the workflow's required role has authority for that node class.
func BehaviorForNodeContract(nodeClass, requiredRole string) (NodeBehavior, error) {
	resolvedClass := ResolveNodeClass("", nodeClass)
	contract, ok := canonicalNodeContracts[resolvedClass]
	if !ok {
		return NodeBehavior{}, fmt.Errorf("node_class %q is not allowed", nodeClass)
	}
	if _, ok := contract.requiredRoles[strings.TrimSpace(requiredRole)]; !ok {
		return NodeBehavior{}, fmt.Errorf("required_role %q is not allowed for node_class %q", requiredRole, resolvedClass)
	}
	return contract.behavior, nil
}

func ExecutionAnalyticsIncludedNodeClasses() []string {
	classes := make([]string, 0, len(canonicalNodeClasses))
	for _, nodeClass := range canonicalNodeClasses {
		if !BehaviorForNode("", nodeClass).IncludeInExecutionAnalytics {
			continue
		}
		classes = append(classes, nodeClass)
	}
	return classes
}

func ExecutionAnalyticsExcludedNodeClasses() []string {
	classes := make([]string, 0, len(canonicalNodeClasses))
	for _, nodeClass := range canonicalNodeClasses {
		if BehaviorForNode("", nodeClass).IncludeInExecutionAnalytics {
			continue
		}
		classes = append(classes, nodeClass)
	}
	return classes
}
