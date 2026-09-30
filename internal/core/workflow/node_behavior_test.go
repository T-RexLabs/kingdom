package workflow

import "testing"

func TestCanonicalWorkflowNodeNamesPreserveExecutionBehavior(t *testing.T) {
	t.Parallel()
	for nodeID, want := range map[string]string{
		"architecture_consultation": NodeClassAdvisorConsultation,
		"risk_consultation":         NodeClassAdvisorConsultation,
		"systems_consultation":      NodeClassAdvisorConsultation,
		"usability_consultation":    NodeClassAdvisorConsultation,
		"implementation":            NodeClassDelegatedExecution,
		"quality_validation":        NodeClassValidationGates,
		"security_validation":       NodeClassValidationGates,
		"resilience_validation":     NodeClassValidationGates,
	} {
		if got := CanonicalNodeClassForNodeID(nodeID); got != want {
			t.Errorf("%s class = %q, want %q", nodeID, got, want)
		}
		if BehaviorForNode(nodeID, "").ExecutionOwnership != NodeExecutionOwnershipProviderBacked {
			t.Errorf("%s lost provider execution ownership", nodeID)
		}
	}
}

func TestCanonicalNodeContractsResolveCompletionModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		nodeClass string
		role      string
		mode      ProviderCompletionMode
	}{
		{NodeClassWorkIntake, "governance", ProviderCompletionModeNotApplicable},
		{NodeClassAdvisorConsultation, "architecture-advisor", ProviderCompletionModeServerResponse},
		{NodeClassAdvisorConsultation, "risk-advisor", ProviderCompletionModeServerResponse},
		{NodeClassAdvisorConsultation, "systems-advisor", ProviderCompletionModeServerResponse},
		{NodeClassAdvisorConsultation, "usability-advisor", ProviderCompletionModeServerResponse},
		{NodeClassAdvisorySynthesis, "coordinator", ProviderCompletionModeServerResponse},
		{NodeClassPlanDrafting, "plan-author", ProviderCompletionModeServerResponse},
		{NodeClassPlanDecision, "governance", ProviderCompletionModeNotApplicable},
		{NodeClassExecutionKickoff, "plan-author", ProviderCompletionModeNotApplicable},
		{NodeClassDelegatedExecution, "builder", ProviderCompletionModeServerResponse},
		{NodeClassExecutionReview, "reviewer", ProviderCompletionModeServerResponse},
		{NodeClassValidationGates, "qa-validator", ProviderCompletionModeServerResponse},
		{NodeClassValidationGates, "security-validator", ProviderCompletionModeServerResponse},
		{NodeClassCompletion, "governance", ProviderCompletionModeNotApplicable},
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
		{NodeClassAdvisorConsultation, "builder"},
		{NodeClassDelegatedExecution, "plan-author"},
		{NodeClassExecutionReview, "qa-validator"},
		{NodeClassExecutionReview, "reviewer-sentinel"},
		{NodeClassValidationGates, "reviewer"},
		{NodeClassValidationGates, "intelligence-runesage"},
		{NodeClassWorkIntake, "coordinator"},
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
		if behavior.ArtifactPersistence != NodeArtifactPersistenceDirect {
			t.Fatalf("expected %s direct artifact persistence, got %q", nodeClass, behavior.ArtifactPersistence)
		}
		if !behavior.ProjectArtifacts {
			t.Fatalf("expected %s to keep canonical response artifact projection enabled", nodeClass)
		}
	}

	builder := BehaviorForNode(NodeClassDelegatedExecution, NodeClassDelegatedExecution)
	if builder.ArtifactPersistence != NodeArtifactPersistenceWorkspace {
		t.Fatalf("expected delegated execution workspace artifact persistence, got %q", builder.ArtifactPersistence)
	}
	if !builder.ProjectArtifacts {
		t.Fatal("expected delegated execution to keep project artifact publication enabled")
	}
}

func TestInspectionResponsesPersistWithoutTheirDisposableWorkspace(t *testing.T) {
	for _, nodeClass := range []string{NodeClassExecutionReview, NodeClassValidationGates} {
		behavior := BehaviorForNode(nodeClass, nodeClass)
		if behavior.ArtifactPersistence != NodeArtifactPersistenceDirect {
			t.Errorf("%s response must persist independently of its disposable workspace: %q", nodeClass, behavior.ArtifactPersistence)
		}
		if !behavior.ProjectArtifacts {
			t.Errorf("%s must retain canonical response artifacts", nodeClass)
		}
	}
}
