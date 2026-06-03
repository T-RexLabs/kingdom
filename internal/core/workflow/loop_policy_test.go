package workflow

import "testing"

func TestNormalizeLoopSettingsPreservesZeroWhenLoopClassAbsent(t *testing.T) {
	settings := NormalizeLoopSettings(LoopSettings{})
	if settings.MaxExecutionReviewCycles != 0 {
		t.Fatalf("expected review cap to remain zero without review nodes, got %d", settings.MaxExecutionReviewCycles)
	}
	if settings.MaxValidationReworkCycles != 0 {
		t.Fatalf("expected validation cap to remain zero without validation nodes, got %d", settings.MaxValidationReworkCycles)
	}
	if settings.RunUntilAllApprove {
		t.Fatal("expected run_until_all_approve to be disabled without execution loops")
	}
}

func TestReadyNodeAutoSucceedsAtLoopCapUsesNormalizedSettings(t *testing.T) {
	settings := NormalizeLoopSettings(LoopSettings{HasExecutionReview: true, MaxExecutionReviewCycles: 1})
	if !ReadyNodeAutoSucceedsAtLoopCap(settings, NodeClassExecutionReview, 2, 1, 0) {
		t.Fatal("expected ready execution review node to auto-succeed once the review cap is reached")
	}
	if !ReadyNodeAutoSucceedsAtLoopCap(settings, NodeClassExecutionReview, 3, 2, 0) {
		t.Fatal("expected ready execution review node to auto-succeed once the review cap is exhausted")
	}

	settings = NormalizeLoopSettings(LoopSettings{HasValidationGates: true, MaxValidationReworkCycles: 1})
	if !ReadyNodeAutoSucceedsAtLoopCap(settings, NodeClassValidationGates, 2, 0, 1) {
		t.Fatal("expected ready validation node to auto-succeed once the validation cap is reached")
	}
	if !ReadyNodeAutoSucceedsAtLoopCap(settings, NodeClassValidationGates, 3, 0, 2) {
		t.Fatal("expected ready validation node to auto-succeed once the validation cap is exhausted")
	}
}

func TestReadyNodeAutoSucceedsImmediatelyWhenLoopCapIsZero(t *testing.T) {
	settings := NormalizeLoopSettings(LoopSettings{HasExecutionReview: true, MaxExecutionReviewCycles: 0})
	if !ReadyNodeAutoSucceedsAtLoopCap(settings, NodeClassExecutionReview, 1, 0, 0) {
		t.Fatal("expected ready execution review node to auto-succeed immediately when review cap is zero")
	}

	settings = NormalizeLoopSettings(LoopSettings{HasValidationGates: true, MaxValidationReworkCycles: 0})
	if !ReadyNodeAutoSucceedsAtLoopCap(settings, NodeClassValidationGates, 1, 0, 0) {
		t.Fatal("expected ready validation node to auto-succeed immediately when validation cap is zero")
	}
}

func TestCanQueueExecutionReviewReworkQueuesCurrentBoundaryFailure(t *testing.T) {
	settings := NormalizeLoopSettings(LoopSettings{HasExecutionReview: true, MaxExecutionReviewCycles: 1})
	queued, capReached := CanQueueExecutionReviewRework(settings, 1)
	if !queued || capReached {
		t.Fatalf("expected current boundary review failure to queue the next wave, got queued=%t capReached=%t", queued, capReached)
	}
	queued, capReached = CanQueueExecutionReviewRework(settings, 2)
	if queued || !capReached {
		t.Fatalf("expected later review failure beyond boundary to stop queueing, got queued=%t capReached=%t", queued, capReached)
	}
}

func TestCanQueueValidationReworkQueuesCurrentBoundaryWave(t *testing.T) {
	settings := NormalizeLoopSettings(LoopSettings{HasValidationGates: true, MaxValidationReworkCycles: 1})
	queued, capReached := CanQueueValidationRework(settings, 1)
	if !queued || capReached {
		t.Fatalf("expected current boundary validation wave to queue the next wave, got queued=%t capReached=%t", queued, capReached)
	}
	queued, capReached = CanQueueValidationRework(settings, 2)
	if queued || !capReached {
		t.Fatalf("expected later validation wave beyond boundary to stop queueing, got queued=%t capReached=%t", queued, capReached)
	}
}

func TestResolveRunLoopSettingsPreservesZeroOverrides(t *testing.T) {
	runUntilAllApprove := true
	zero := 0
	settings := ResolveRunLoopSettings(Definition{
		Nodes:    []Node{{NodeID: "builder", NodeClass: NodeClassDelegatedExecution}, {NodeID: "review", NodeClass: NodeClassExecutionReview}, {NodeID: "validation", NodeClass: NodeClassValidationGates}},
		Metadata: Metadata{MaxExecutionReviewCycles: refInt(5), MaxValidationReworkCycles: refInt(7)},
	}, LoopSettingsOverrides{
		RunUntilAllApprove:        &runUntilAllApprove,
		MaxExecutionReviewCycles:  &zero,
		MaxValidationReworkCycles: &zero,
	})
	if !settings.RunUntilAllApprove {
		t.Fatal("expected run_until_all_approve override to be preserved when execution loops exist")
	}
	if settings.MaxExecutionReviewCycles != 0 {
		t.Fatalf("expected zero review override to be preserved, got %d", settings.MaxExecutionReviewCycles)
	}
	if settings.MaxValidationReworkCycles != 0 {
		t.Fatalf("expected zero validation override to be preserved, got %d", settings.MaxValidationReworkCycles)
	}
}

func TestResolveRunLoopSettingsUsesPositiveOverridesWhenProvided(t *testing.T) {
	reviewOverride := 3
	validationOverride := 4
	settings := ResolveRunLoopSettings(Definition{
		Nodes:    []Node{{NodeID: "builder", NodeClass: NodeClassDelegatedExecution}, {NodeID: "review", NodeClass: NodeClassExecutionReview}, {NodeID: "validation", NodeClass: NodeClassValidationGates}},
		Metadata: Metadata{MaxExecutionReviewCycles: refInt(5), MaxValidationReworkCycles: refInt(7)},
	}, LoopSettingsOverrides{
		MaxExecutionReviewCycles:  &reviewOverride,
		MaxValidationReworkCycles: &validationOverride,
	})
	if settings.MaxExecutionReviewCycles != 3 {
		t.Fatalf("expected positive review override 3, got %d", settings.MaxExecutionReviewCycles)
	}
	if settings.MaxValidationReworkCycles != 4 {
		t.Fatalf("expected positive validation override 4, got %d", settings.MaxValidationReworkCycles)
	}
}
