package workflow

import "strings"

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

type NodeBehavior struct {
	ExecutionOwnership           NodeExecutionOwnership
	RequiresWorkspace            bool
	ArtifactPersistence          NodeArtifactPersistence
	ProjectArtifacts             bool
	IncludeInExecutionAnalytics  bool
	WaitsForRecordedPlanDecision bool
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
	switch ResolveNodeClass(nodeID, nodeClass) {
	case NodeClassDecreeIntake:
		return NodeBehavior{
			ExecutionOwnership:          NodeExecutionOwnershipRuntimeOwned,
			RequiresWorkspace:           false,
			ArtifactPersistence:         NodeArtifactPersistenceDirect,
			ProjectArtifacts:            false,
			IncludeInExecutionAnalytics: false,
		}
	case NodeClassKingPlanDecision:
		return NodeBehavior{
			ExecutionOwnership:           NodeExecutionOwnershipRuntimeOwned,
			RequiresWorkspace:            false,
			ArtifactPersistence:          NodeArtifactPersistenceDirect,
			ProjectArtifacts:             false,
			IncludeInExecutionAnalytics:  false,
			WaitsForRecordedPlanDecision: true,
		}
	case NodeClassExecutionKickoff:
		return NodeBehavior{
			ExecutionOwnership:          NodeExecutionOwnershipRuntimeOwned,
			RequiresWorkspace:           false,
			ArtifactPersistence:         NodeArtifactPersistenceDirect,
			ProjectArtifacts:            false,
			IncludeInExecutionAnalytics: false,
		}
	case NodeClassRoyalClosure:
		return NodeBehavior{
			ExecutionOwnership:          NodeExecutionOwnershipRuntimeOwned,
			RequiresWorkspace:           false,
			ArtifactPersistence:         NodeArtifactPersistenceDirect,
			ProjectArtifacts:            false,
			IncludeInExecutionAnalytics: false,
		}
	default:
		return NodeBehavior{
			ExecutionOwnership:          NodeExecutionOwnershipProviderBacked,
			RequiresWorkspace:           true,
			ArtifactPersistence:         NodeArtifactPersistenceWorkspace,
			ProjectArtifacts:            true,
			IncludeInExecutionAnalytics: true,
		}
	}
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
