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
	RequiresWorkspace            bool
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
	NodeClassDecreeIntake: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipRuntimeOwned, ProviderCompletionMode: ProviderCompletionModeNotApplicable, ArtifactPersistence: NodeArtifactPersistenceDirect},
		requiredRoles: roleSet("king"),
	},
	NodeClassAdvisorConsultation: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipProviderBacked, ProviderCompletionMode: ProviderCompletionModeServerResponse, ArtifactPersistence: NodeArtifactPersistenceDirect, ProjectArtifacts: true, IncludeInExecutionAnalytics: true},
		requiredRoles: roleSet("noble-delegate", "clergy-delegate", "burgher-delegate", "peasant-delegate"),
	},
	NodeClassAdvisorySynthesis: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipProviderBacked, ProviderCompletionMode: ProviderCompletionModeServerResponse, ArtifactPersistence: NodeArtifactPersistenceDirect, ProjectArtifacts: true, IncludeInExecutionAnalytics: true},
		requiredRoles: roleSet("chancellor"),
	},
	NodeClassPlanDrafting: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipProviderBacked, ProviderCompletionMode: ProviderCompletionModeServerResponse, ArtifactPersistence: NodeArtifactPersistenceDirect, ProjectArtifacts: true, IncludeInExecutionAnalytics: true},
		requiredRoles: roleSet("council-member"),
	},
	NodeClassKingPlanDecision: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipRuntimeOwned, ProviderCompletionMode: ProviderCompletionModeNotApplicable, ArtifactPersistence: NodeArtifactPersistenceDirect, WaitsForRecordedPlanDecision: true},
		requiredRoles: roleSet("king"),
	},
	NodeClassExecutionKickoff: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipRuntimeOwned, ProviderCompletionMode: ProviderCompletionModeNotApplicable, ArtifactPersistence: NodeArtifactPersistenceDirect},
		requiredRoles: roleSet("council-member"),
	},
	NodeClassDelegatedExecution: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipProviderBacked, ProviderCompletionMode: ProviderCompletionModeServerResponse, RequiresWorkspace: true, ArtifactPersistence: NodeArtifactPersistenceWorkspace, ProjectArtifacts: true, IncludeInExecutionAnalytics: true},
		requiredRoles: roleSet("builder-berserker"),
	},
	NodeClassExecutionReview: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipProviderBacked, ProviderCompletionMode: ProviderCompletionModeServerResponse, RequiresWorkspace: true, ArtifactPersistence: NodeArtifactPersistenceWorkspace, ProjectArtifacts: true, IncludeInExecutionAnalytics: true},
		requiredRoles: roleSet("reviewer-shieldmaiden"),
	},
	NodeClassValidationGates: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipProviderBacked, ProviderCompletionMode: ProviderCompletionModeServerResponse, RequiresWorkspace: true, ArtifactPersistence: NodeArtifactPersistenceWorkspace, ProjectArtifacts: true, IncludeInExecutionAnalytics: true},
		requiredRoles: roleSet("intelligence-qa-scout", "intelligence-nightraven", "intelligence-loki-chaos"),
	},
	NodeClassRoyalClosure: {
		behavior:      NodeBehavior{ExecutionOwnership: NodeExecutionOwnershipRuntimeOwned, ProviderCompletionMode: ProviderCompletionModeNotApplicable, ArtifactPersistence: NodeArtifactPersistenceDirect},
		requiredRoles: roleSet("king"),
	},
}

func CanonicalNodeClassForNodeID(nodeID string) string {
	value := strings.TrimSpace(strings.ToLower(nodeID))
	for _, classID := range []string{
		NodeClassDecreeIntake,
		NodeClassAdvisorConsultation,
		NodeClassAdvisorySynthesis,
		NodeClassPlanDrafting,
		NodeClassKingPlanDecision,
		NodeClassExecutionKickoff,
		NodeClassDelegatedExecution,
		NodeClassExecutionReview,
		NodeClassValidationGates,
		NodeClassRoyalClosure,
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
