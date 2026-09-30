package orchestration

import (
	coreworkflow "kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
)

type nodeArtifactPersistenceMode string

// Workspace kind is fixed by the canonical node class, never provider input.
// Only canonical workspaces can change the run's trusted implementation tree.
type executionWorkspaceKind string

const (
	workspaceNone            executionWorkspaceKind = "none"
	workspaceCanonical       executionWorkspaceKind = "canonical"
	workspacePrivateSource   executionWorkspaceKind = "private_source"
	workspacePrivateSnapshot executionWorkspaceKind = "private_snapshot"
)

type nodeExecutionSpec struct {
	Mode                executionstore.ExecutionMode
	CompletionMode      executionstore.ProviderCompletionMode
	WorkspaceKind       executionWorkspaceKind
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
	workspaceKind := workspacePrivateSource
	if mode == executionstore.ExecutionModeRuntimeOwned {
		workspaceKind = workspaceNone
	} else {
		switch coreworkflow.ResolveNodeClass(nodeID, nodeClass) {
		case coreworkflow.NodeClassDelegatedExecution:
			workspaceKind = workspaceCanonical
		case coreworkflow.NodeClassExecutionReview, coreworkflow.NodeClassValidationGates:
			workspaceKind = workspacePrivateSnapshot
		}
	}
	return nodeExecutionSpec{
		Mode:                mode,
		CompletionMode:      completionMode,
		WorkspaceKind:       workspaceKind,
		ArtifactPersistence: persistence,
		ProjectArtifacts:    behavior.ProjectArtifacts,
	}
}

func executionModeForNode(nodeID, nodeClass string) executionstore.ExecutionMode {
	return executionSpecForNode(nodeID, nodeClass).Mode
}
