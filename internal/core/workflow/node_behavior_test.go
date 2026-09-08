package workflow

import "testing"

func TestCanonicalNodeContractsResolveCompletionModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		nodeClass string
		role      string
		mode      ProviderCompletionMode
	}{
		{NodeClassDecreeIntake, "king", ProviderCompletionModeNotApplicable},
		{NodeClassAdvisorConsultation, "noble-delegate", ProviderCompletionModeServerResponse},
		{NodeClassAdvisorConsultation, "clergy-delegate", ProviderCompletionModeServerResponse},
		{NodeClassAdvisorConsultation, "burgher-delegate", ProviderCompletionModeServerResponse},
		{NodeClassAdvisorConsultation, "peasant-delegate", ProviderCompletionModeServerResponse},
		{NodeClassAdvisorySynthesis, "chancellor", ProviderCompletionModeServerResponse},
		{NodeClassPlanDrafting, "council-member", ProviderCompletionModeServerResponse},
		{NodeClassKingPlanDecision, "king", ProviderCompletionModeNotApplicable},
		{NodeClassExecutionKickoff, "council-member", ProviderCompletionModeNotApplicable},
		{NodeClassDelegatedExecution, "builder-berserker", ProviderCompletionModeReceiptRequired},
		{NodeClassExecutionReview, "reviewer-shieldmaiden", ProviderCompletionModeServerResponse},
		{NodeClassValidationGates, "intelligence-qa-scout", ProviderCompletionModeServerResponse},
		{NodeClassValidationGates, "intelligence-nightraven", ProviderCompletionModeServerResponse},
		{NodeClassRoyalClosure, "king", ProviderCompletionModeNotApplicable},
	}

	for _, test := range tests {
		test := test
		t.Run(test.nodeClass+"/"+test.role, func(t *testing.T) {
			t.Parallel()
			behavior, err := BehaviorForNodeContract(test.nodeClass, test.role)
			if err != nil {
				t.Fatalf("resolve node contract: %v", err)
			}
			if behavior.ProviderCompletionMode != test.mode {
				t.Fatalf("completion mode = %q, want %q", behavior.ProviderCompletionMode, test.mode)
			}
		})
	}
}

func TestCanonicalNodeContractsRejectInvalidRolePairs(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		nodeClass string
		role      string
	}{
		{NodeClassAdvisorConsultation, "builder-berserker"},
		{NodeClassDelegatedExecution, "council-member"},
		{NodeClassExecutionReview, "intelligence-qa-scout"},
		{NodeClassExecutionReview, "reviewer-sentinel"},
		{NodeClassValidationGates, "reviewer-shieldmaiden"},
		{NodeClassValidationGates, "intelligence-runesage"},
		{NodeClassDecreeIntake, "chancellor"},
	} {
		test := test
		t.Run(test.nodeClass+"/"+test.role, func(t *testing.T) {
			t.Parallel()
			if _, err := BehaviorForNodeContract(test.nodeClass, test.role); err == nil {
				t.Fatal("expected invalid node-class and required-role pair to be rejected")
			}
		})
	}
}

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
