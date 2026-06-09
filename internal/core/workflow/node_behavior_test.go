package workflow

import "testing"

func TestBehaviorForResponseFirstPlanningNodes(t *testing.T) {
	t.Parallel()

	for _, nodeClass := range []string{NodeClassAdvisorConsultation, NodeClassAdvisorySynthesis, NodeClassPlanDrafting} {
		behavior := BehaviorForNode(nodeClass, nodeClass)
		if behavior.ExecutionOwnership != NodeExecutionOwnershipProviderBacked {
			t.Fatalf("expected %s to stay provider-backed, got %q", nodeClass, behavior.ExecutionOwnership)
		}
		if behavior.RequiresWorkspace {
			t.Fatalf("expected %s to avoid workspace preparation", nodeClass)
		}
		if behavior.ArtifactPersistence != NodeArtifactPersistenceDirect {
			t.Fatalf("expected %s direct artifact persistence, got %q", nodeClass, behavior.ArtifactPersistence)
		}
		if !behavior.ProjectArtifacts {
			t.Fatalf("expected %s to keep canonical response artifact projection enabled", nodeClass)
		}
	}

	builder := BehaviorForNode(NodeClassDelegatedExecution, NodeClassDelegatedExecution)
	if !builder.RequiresWorkspace {
		t.Fatal("expected delegated execution to remain workspace-backed")
	}
	if builder.ArtifactPersistence != NodeArtifactPersistenceWorkspace {
		t.Fatalf("expected delegated execution workspace artifact persistence, got %q", builder.ArtifactPersistence)
	}
	if !builder.ProjectArtifacts {
		t.Fatal("expected delegated execution to keep project artifact publication enabled")
	}
}
