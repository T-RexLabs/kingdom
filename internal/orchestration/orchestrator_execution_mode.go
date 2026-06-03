package orchestration

import (
	coreworkflow "kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
)

type nodeArtifactPersistenceMode string

type nodeExecutionSpec struct {
	Mode                executionstore.ExecutionMode
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
	if behavior.ExecutionOwnership == coreworkflow.NodeExecutionOwnershipRuntimeOwned {
		mode = executionstore.ExecutionModeRuntimeOwned
	}
	persistence := nodeArtifactPersistenceWorkspace
	if behavior.ArtifactPersistence == coreworkflow.NodeArtifactPersistenceDirect {
		persistence = nodeArtifactPersistenceDirect
	}
	return nodeExecutionSpec{
		Mode:                mode,
		RequiresWorkspace:   behavior.RequiresWorkspace,
		ArtifactPersistence: persistence,
		ProjectArtifacts:    behavior.ProjectArtifacts,
	}
}

func executionModeForNode(nodeID, nodeClass string) executionstore.ExecutionMode {
	return executionSpecForNode(nodeID, nodeClass).Mode
}
