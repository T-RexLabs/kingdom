package orchestration

import (
	coreworkflow "kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
)

type nodeArtifactPersistenceMode string

type nodeExecutionSpec struct {
	Mode                executionstore.ExecutionMode
	CompletionMode      executionstore.ProviderCompletionMode
	RequiresWorkspace   bool
	ArtifactPersistence nodeArtifactPersistenceMode
	ProjectArtifacts    bool
}

const (
	nodeArtifactPersistenceWorkspace nodeArtifactPersistenceMode = "workspace"
	nodeArtifactPersistenceDirect    nodeArtifactPersistenceMode = "direct"
)

func executionSpecForNode(nodeID, nodeClass string) nodeExecutionSpec {
	behavior := coreworkflow.BehaviorForNode(nodeID, nodeClass)
	mode := executionstore.ExecutionModeProviderBacked
	completionMode := executionstore.ProviderCompletionMode(behavior.ProviderCompletionMode)
	if behavior.ExecutionOwnership == coreworkflow.NodeExecutionOwnershipRuntimeOwned {
		mode = executionstore.ExecutionModeRuntimeOwned
		completionMode = executionstore.ProviderCompletionModeNotApplicable
	}
	persistence := nodeArtifactPersistenceWorkspace
	if behavior.ArtifactPersistence == coreworkflow.NodeArtifactPersistenceDirect {
		persistence = nodeArtifactPersistenceDirect
	}
	return nodeExecutionSpec{
		Mode:                mode,
		CompletionMode:      completionMode,
		RequiresWorkspace:   behavior.RequiresWorkspace,
		ArtifactPersistence: persistence,
		ProjectArtifacts:    behavior.ProjectArtifacts,
	}
}

func executionModeForNode(nodeID, nodeClass string) executionstore.ExecutionMode {
	return executionSpecForNode(nodeID, nodeClass).Mode
}
