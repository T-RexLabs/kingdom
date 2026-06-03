package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/workflow"
)

func TestServerReplaysIdempotentMutations(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)

	first := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/decrees",
		body: map[string]any{
			"decree_title": "Replay",
			"decree_text":  "Same body",
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-replay",
	})
	second := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/decrees",
		body: map[string]any{
			"decree_title": "Replay",
			"decree_text":  "Same body",
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-replay",
	})

	if first.Code != http.StatusCreated || second.Code != http.StatusCreated {
		t.Fatalf("expected 201 responses, got first=%d second=%d", first.Code, second.Code)
	}

	var firstBody gen.CreateDecreeResult
	if err := json.Unmarshal(first.Body.Bytes(), &firstBody); err != nil {
		t.Fatalf("decode first response: %v", err)
	}
	var secondBody gen.CreateDecreeResult
	if err := json.Unmarshal(second.Body.Bytes(), &secondBody); err != nil {
		t.Fatalf("decode second response: %v", err)
	}

	if firstBody.DecreeId != secondBody.DecreeId {
		t.Fatalf("expected same decree id on replay, got first=%q second=%q", firstBody.DecreeId, secondBody.DecreeId)
	}

	if !firstBody.CreatedAt.UTC().Equal(firstBody.CreatedAt) {
		t.Fatalf("expected RFC3339 UTC timestamp, got %s", firstBody.CreatedAt)
	}
}

func TestServerPublishWorkflowRejectsInvalidDefinition(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": "wf-invalid-v1",
			"version":     "v1",
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": "wf-invalid-v1",
				"nodes": []map[string]any{{
					"node_id":       "kickoff",
					"required_role": "non-existent-role",
					"node_class":    "execution_kickoff",
				}},
				"edges": []map[string]any{},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-invalid-workflow",
	})

	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (%s)", res.Code, res.Body.String())
	}
	assertErrorCode(t, res.Body.Bytes(), "policy_violation")
}

func TestPersistWorkflowGraphRejectsInvalidDefinitionJSON(t *testing.T) {
	t.Parallel()

	err := (&Server{}).persistWorkflowGraph(context.Background(), "wfdef-invalid", []byte("{"), "2026-03-02T09:00:00Z")
	if err == nil {
		t.Fatal("expected persistWorkflowGraph to reject invalid workflow definition JSON")
	}
}

func TestServerPublishWorkflowRejectsUnknownDefinitionFields(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": "wf-unknown-field-v1",
			"version":     "v1",
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": "wf-unknown-field-v1",
				"nodes": []map[string]any{{
					"node_id":       "kickoff",
					"required_role": "council-member",
					"node_class":    "execution_kickoff",
					"unexpected":    true,
				}},
				"edges": []map[string]any{},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-workflow-unknown-field",
	})

	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (%s)", res.Code, res.Body.String())
	}
	assertErrorCode(t, res.Body.Bytes(), "policy_violation")
}

func TestServerPublishWorkflowRejectsInvalidNodeClass(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": "wf-invalid-node-class-v1",
			"version":     "v1",
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": "wf-invalid-node-class-v1",
				"nodes": []map[string]any{{
					"node_id":       "kickoff",
					"required_role": "council-member",
					"node_class":    "totally_unknown_class",
				}},
				"edges": []map[string]any{},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-workflow-invalid-node-class",
	})

	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (%s)", res.Code, res.Body.String())
	}
	assertErrorCode(t, res.Body.Bytes(), "policy_violation")
}

func TestServerCreateRunPreservesPublishedWorkflowIdentity(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: repoRootForAPITests(t)})

	decreeRes := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/decrees",
		body:           map[string]any{"decree_title": "D", "decree_text": "T"},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-rework-decree",
	})
	if decreeRes.Code != http.StatusCreated {
		t.Fatalf("expected decree 201, got %d", decreeRes.Code)
	}
	var decree gen.CreateDecreeResult
	if err := json.Unmarshal(decreeRes.Body.Bytes(), &decree); err != nil {
		t.Fatalf("decode decree: %v", err)
	}

	publishRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": workflow.DefaultWorkflowID,
			"version":     "rework-v1",
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": workflow.DefaultWorkflowID,
				"nodes": []map[string]any{{
					"node_id":       "kickoff",
					"required_role": "council-member",
					"node_class":    "execution_kickoff",
				}},
				"edges": []map[string]any{},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-rework-workflow",
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("expected publish 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
	}

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"decree_id":       decree.DecreeId,
			"publish_mode":    "direct",
			"model_selection": defaultTestRunModelSelectionBody(),
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-rework-run",
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("expected run 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}

	var run gen.RunSummary
	if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run: %v", err)
	}
	if run.DecreeTitle != "D" {
		t.Fatalf("expected decree title D, got %q", run.DecreeTitle)
	}
}

func TestServerCreateRunPersistsLoopModeAndCycleOverrides(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: repoRootForAPITests(t)})

	decreeRes := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/decrees",
		body:           map[string]any{"decree_title": "Loop", "decree_text": "Mode"},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-loop-mode-decree",
	})
	if decreeRes.Code != http.StatusCreated {
		t.Fatalf("expected decree 201, got %d", decreeRes.Code)
	}
	var decree gen.CreateDecreeResult
	if err := json.Unmarshal(decreeRes.Body.Bytes(), &decree); err != nil {
		t.Fatalf("decode decree: %v", err)
	}

	publishRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": workflow.DefaultWorkflowID,
			"version":     "loop-mode-v1",
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": workflow.DefaultWorkflowID,
				"nodes": []map[string]any{
					{
						"node_id":       "builder",
						"required_role": "builder-berserker",
						"node_class":    "delegated_execution",
					},
					{
						"node_id":       "review",
						"required_role": "reviewer-shieldmaiden",
						"node_class":    "execution_review",
					},
					{
						"node_id":       "validation",
						"required_role": "intelligence-qa-scout",
						"node_class":    "validation_gates",
					},
				},
				"edges": []map[string]any{
					{"from": "builder", "to": "review", "on": "success"},
					{"from": "review", "to": "validation", "on": "success"},
				},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-loop-mode-workflow",
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("expected publish 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
	}

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"decree_id":                    decree.DecreeId,
			"publish_mode":                 "direct",
			"model_selection":              defaultTestRunModelSelectionBody(),
			"run_until_all_approve":        true,
			"max_execution_review_cycles":  7,
			"max_validation_rework_cycles": 9,
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-loop-mode-run",
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("expected run 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}

	var run gen.RunSummary
	if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run: %v", err)
	}
	if run.RunUntilAllApprove == nil || !*run.RunUntilAllApprove {
		t.Fatalf("expected run summary run_until_all_approve=true, got %#v", run.RunUntilAllApprove)
	}
	if run.AutonomousApprovals == nil || !*run.AutonomousApprovals {
		t.Fatalf("expected run summary autonomous_approvals=true by default, got %#v", run.AutonomousApprovals)
	}
	if run.MaxExecutionReviewCycles == nil || *run.MaxExecutionReviewCycles != 7 {
		t.Fatalf("expected run summary max_execution_review_cycles=7, got %#v", run.MaxExecutionReviewCycles)
	}
	if run.MaxValidationReworkCycles == nil || *run.MaxValidationReworkCycles != 9 {
		t.Fatalf("expected run summary max_validation_rework_cycles=9, got %#v", run.MaxValidationReworkCycles)
	}

	var storedAutonomousApprovals, storedUntilAllApprove, storedReviewCycles, storedValidationCycles int
	if err := db.QueryRow(`SELECT autonomous_approvals, run_until_all_approve, max_execution_review_cycles, max_validation_rework_cycles FROM workflow_runs WHERE run_id = ?`, run.RunId).Scan(&storedAutonomousApprovals, &storedUntilAllApprove, &storedReviewCycles, &storedValidationCycles); err != nil {
		t.Fatalf("query run loop-mode fields: %v", err)
	}
	if storedAutonomousApprovals != 1 || storedUntilAllApprove != 1 || storedReviewCycles != 7 || storedValidationCycles != 9 {
		t.Fatalf("unexpected stored loop mode values: autonomous=%d until=%d review=%d validation=%d", storedAutonomousApprovals, storedUntilAllApprove, storedReviewCycles, storedValidationCycles)
	}
}

func TestServerCreateRunPersistsPublishMode(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: repoRootForAPITests(t)})

	decreeID := seedDecree(t, handler)
	seedWorkflow(t, handler, workflow.DefaultWorkflowID)

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"decree_id":       decreeID,
			"publish_mode":    "git_worktree",
			"model_selection": defaultTestRunModelSelectionBody(),
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-publish-mode-run",
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("expected run 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}

	var run gen.RunSummary
	if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run summary: %v", err)
	}
	if run.PublishMode == nil || *run.PublishMode != gen.GitWorktree {
		t.Fatalf("expected response publish_mode=git_worktree, got %#v", run.PublishMode)
	}

	var storedPublishMode string
	if err := db.QueryRow(`SELECT publish_mode FROM workflow_runs WHERE run_id = ?`, run.RunId).Scan(&storedPublishMode); err != nil {
		t.Fatalf("query publish_mode: %v", err)
	}
	if storedPublishMode != "git_worktree" {
		t.Fatalf("expected stored publish_mode=git_worktree, got %q", storedPublishMode)
	}
}

func TestServerCreateRunDefaultsReviewLoopsToZeroWhenWorkflowOmitsQualityGates(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: repoRootForAPITests(t)})

	decreeID := seedDecree(t, handler)
	publishRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": workflow.SkipReviewValidationWorkflowID,
			"version":     "no-quality-gates-v1",
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": workflow.SkipReviewValidationWorkflowID,
				"nodes": []map[string]any{
					{
						"node_id":       "kickoff",
						"required_role": "council-member",
						"node_class":    "execution_kickoff",
					},
					{
						"node_id":       "builder",
						"required_role": "builder-berserker",
						"node_class":    "delegated_execution",
					},
					{
						"node_id":       "close",
						"required_role": "king",
						"node_class":    "royal_closure",
					},
				},
				"edges": []map[string]any{
					{"from": "kickoff", "to": "builder", "on": "success"},
					{"from": "builder", "to": "close", "on": "success"},
				},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-no-quality-gates-workflow",
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("expected publish 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
	}

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"decree_id":                    decreeID,
			"publish_mode":                 "direct",
			"skip_reviews_and_validations": true,
			"model_selection":              defaultTestRunModelSelectionBody(),
			"run_until_all_approve":        true,
			"max_execution_review_cycles":  9,
			"max_validation_rework_cycles": 11,
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-no-quality-gates-run",
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("expected run 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}

	var run gen.RunSummary
	if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run summary: %v", err)
	}
	if run.RunUntilAllApprove == nil || *run.RunUntilAllApprove {
		t.Fatalf("expected no-loop workflow to normalize run_until_all_approve=false, got %#v", run.RunUntilAllApprove)
	}
	if run.MaxExecutionReviewCycles == nil || *run.MaxExecutionReviewCycles != 0 {
		t.Fatalf("expected no-loop workflow to normalize max_execution_review_cycles=0, got %#v", run.MaxExecutionReviewCycles)
	}
	if run.MaxValidationReworkCycles == nil || *run.MaxValidationReworkCycles != 0 {
		t.Fatalf("expected no-loop workflow to normalize max_validation_rework_cycles=0, got %#v", run.MaxValidationReworkCycles)
	}

	var storedUntilAllApprove, storedReviewCycles, storedValidationCycles int
	if err := db.QueryRow(`SELECT run_until_all_approve, max_execution_review_cycles, max_validation_rework_cycles FROM workflow_runs WHERE run_id = ?`, run.RunId).Scan(&storedUntilAllApprove, &storedReviewCycles, &storedValidationCycles); err != nil {
		t.Fatalf("query normalized no-quality-gates loop settings: %v", err)
	}
	if storedUntilAllApprove != 0 || storedReviewCycles != 0 || storedValidationCycles != 0 {
		t.Fatalf("expected no-loop workflow to persist normalized loop settings, got until=%d review=%d validation=%d", storedUntilAllApprove, storedReviewCycles, storedValidationCycles)
	}
}

func TestServerCreateRunPersistsZeroCycleOverridesWhenQualityGatesExist(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: repoRootForAPITests(t)})

	decreeID := seedDecree(t, handler)
	publishRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": workflow.DefaultWorkflowID,
			"version":     "loop-mode-zero-v1",
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": workflow.DefaultWorkflowID,
				"metadata": map[string]any{
					"max_execution_review_cycles":  5,
					"max_validation_rework_cycles": 7,
				},
				"nodes": []map[string]any{
					{
						"node_id":       "builder",
						"required_role": "builder-berserker",
						"node_class":    "delegated_execution",
					},
					{
						"node_id":       "review",
						"required_role": "reviewer-shieldmaiden",
						"node_class":    "execution_review",
					},
					{
						"node_id":       "validation",
						"required_role": "intelligence-qa-scout",
						"node_class":    "validation_gates",
					},
				},
				"edges": []map[string]any{
					{"from": "builder", "to": "review", "on": "success"},
					{"from": "review", "to": "validation", "on": "success"},
				},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-loop-mode-zero-workflow",
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("expected publish 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
	}

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"decree_id":                    decreeID,
			"publish_mode":                 "direct",
			"model_selection":              defaultTestRunModelSelectionBody(),
			"max_execution_review_cycles":  0,
			"max_validation_rework_cycles": 0,
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-loop-mode-zero-run",
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("expected run 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}

	var run gen.RunSummary
	if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run summary: %v", err)
	}
	if run.MaxExecutionReviewCycles == nil || *run.MaxExecutionReviewCycles != 0 {
		t.Fatalf("expected zero review cap override to be preserved, got %#v", run.MaxExecutionReviewCycles)
	}
	if run.MaxValidationReworkCycles == nil || *run.MaxValidationReworkCycles != 0 {
		t.Fatalf("expected zero validation cap override to be preserved, got %#v", run.MaxValidationReworkCycles)
	}

	var storedReviewCycles, storedValidationCycles int
	if err := db.QueryRow(`SELECT max_execution_review_cycles, max_validation_rework_cycles FROM workflow_runs WHERE run_id = ?`, run.RunId).Scan(&storedReviewCycles, &storedValidationCycles); err != nil {
		t.Fatalf("query normalized loop caps: %v", err)
	}
	if storedReviewCycles != 0 || storedValidationCycles != 0 {
		t.Fatalf("expected zero loop caps to be persisted, got review=%d validation=%d", storedReviewCycles, storedValidationCycles)
	}
}

func TestServerCreateRunPersistsModelSelection(t *testing.T) {
	t.Parallel()

	mockCatalog := staticProviderModelCatalog{items: []gen.ProviderModel{
		{
			Id:           "github-copilot/gpt-5.3-codex",
			ProviderId:   "github-copilot",
			ProviderName: "GitHub Copilot",
			ModelName:    "GPT-5.3-Codex",
			ThinkingEffortOptions: []gen.ThinkingEffortOption{
				{Value: "low", Label: "Low"},
				{Value: "medium", Label: "Medium"},
				{Value: "high", Label: "High"},
			},
		},
		{
			Id:                    "opencode/gpt-5-nano",
			ProviderId:            "opencode",
			ProviderName:          "OpenCode",
			ModelName:             "GPT-5 Nano",
			ThinkingEffortDefault: refString("medium"),
			ThinkingEffortOptions: []gen.ThinkingEffortOption{
				{Value: "minimal", Label: "Minimal"},
				{Value: "medium", Label: "Medium"},
				{Value: "high", Label: "High"},
			},
		},
	}}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProviderModelCatalog: mockCatalog})

	decreeRes := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/decrees",
		body:           map[string]any{"decree_title": "Model", "decree_text": "Selection"},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-model-select-decree",
	})
	if decreeRes.Code != http.StatusCreated {
		t.Fatalf("expected decree 201, got %d", decreeRes.Code)
	}
	var decree gen.CreateDecreeResult
	if err := json.Unmarshal(decreeRes.Body.Bytes(), &decree); err != nil {
		t.Fatalf("decode decree: %v", err)
	}

	publishRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": workflow.DefaultWorkflowID,
			"version":     "model-select-v1",
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": workflow.DefaultWorkflowID,
				"nodes": []map[string]any{{
					"node_id":       "kickoff",
					"required_role": "council-member",
					"node_class":    "execution_kickoff",
				}},
				"edges": []map[string]any{},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-model-select-workflow",
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("expected publish 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
	}

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"decree_id":    decree.DecreeId,
			"publish_mode": "direct",
			"model_selection": map[string]any{
				"global": map[string]any{
					"model":           "github-copilot/gpt-5.3-codex",
					"thinking_effort": "high",
				},
				"role_overrides": map[string]any{
					"reviewer-shieldmaiden": map[string]any{
						"model":           "opencode/gpt-5-nano",
						"thinking_effort": "minimal",
					},
				},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-model-select-run",
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("expected run 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}

	var summary gen.RunSummary
	if err := json.Unmarshal(runRes.Body.Bytes(), &summary); err != nil {
		t.Fatalf("decode run summary: %v", err)
	}
	if summary.ModelSelection == nil || summary.ModelSelection.Global.Model != "github-copilot/gpt-5.3-codex" {
		t.Fatalf("expected global model selection in response, got %#v", summary.ModelSelection)
	}
	if summary.ModelSelection.Global.ThinkingEffort == nil || *summary.ModelSelection.Global.ThinkingEffort != "high" {
		t.Fatalf("expected global thinking effort in response, got %#v", summary.ModelSelection.Global)
	}
	if summary.ModelSelection.RoleOverrides == nil || (*summary.ModelSelection.RoleOverrides)["reviewer-shieldmaiden"].Model != "opencode/gpt-5-nano" {
		t.Fatalf("expected reviewer role override in response, got %#v", summary.ModelSelection)
	}
	if effort := (*summary.ModelSelection.RoleOverrides)["reviewer-shieldmaiden"].ThinkingEffort; effort == nil || *effort != "minimal" {
		t.Fatalf("expected reviewer thinking effort in response, got %#v", summary.ModelSelection)
	}

	var storedSelectionRaw string
	if err := db.QueryRow(`SELECT COALESCE(model_selection_json, '') FROM workflow_runs WHERE run_id = ?`, summary.RunId).Scan(&storedSelectionRaw); err != nil {
		t.Fatalf("query run model selection: %v", err)
	}
	var storedSelection gen.RunModelSelection
	if err := json.Unmarshal([]byte(storedSelectionRaw), &storedSelection); err != nil {
		t.Fatalf("decode stored selection: %v", err)
	}
	if storedSelection.Global.Model != "github-copilot/gpt-5.3-codex" {
		t.Fatalf("expected stored global selection, got %#v", storedSelection)
	}
	if storedSelection.RoleOverrides == nil || (*storedSelection.RoleOverrides)["reviewer-shieldmaiden"].Model != "opencode/gpt-5-nano" {
		t.Fatalf("expected stored reviewer selection, got %#v", storedSelection)
	}
}

func TestServerCreateRunRejectsUnknownAgentModelOverrideRole(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)
	decreeID := seedDecree(t, handler)
	seedWorkflow(t, handler, workflow.DefaultWorkflowID)

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"decree_id":    decreeID,
			"publish_mode": "direct",
			"model_selection": map[string]any{
				"role_overrides": map[string]any{
					"unknown-role": map[string]any{
						"model": "github-copilot/gpt-5.3-codex",
					},
				},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-model-role-check",
	})
	if runRes.Code != http.StatusBadRequest {
		t.Fatalf("expected run 400 for unknown role override, got %d (%s)", runRes.Code, runRes.Body.String())
	}
}

func TestServerCreateRunRequiresModelSelectionWhenProviderEngineIsOpenCode(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)
	decreeID := seedDecree(t, handler)
	seedWorkflow(t, handler, workflow.DefaultWorkflowID)

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"decree_id":    decreeID,
			"publish_mode": "direct",
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-model-required",
	})
	if runRes.Code != http.StatusBadRequest {
		t.Fatalf("expected run 400 for missing global model in opencode mode, got %d (%s)", runRes.Code, runRes.Body.String())
	}
	if !strings.Contains(runRes.Body.String(), "model_selection") {
		t.Fatalf("expected missing model_selection validation message, got %s", runRes.Body.String())
	}
}

func TestServerCreateRunRejectsUnavailableGlobalModel(t *testing.T) {
	t.Parallel()

	mockCatalog := staticProviderModelCatalog{items: []gen.ProviderModel{{
		Id:                    "github-copilot/gpt-5.3-codex",
		ProviderId:            "github-copilot",
		ProviderName:          "GitHub Copilot",
		ModelName:             "GPT-5.3-Codex",
		ThinkingEffortOptions: []gen.ThinkingEffortOption{{Value: "low", Label: "Low"}},
	}}}
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ProviderModelCatalog: mockCatalog})
	decreeID := seedDecree(t, handler)
	seedWorkflow(t, handler, workflow.DefaultWorkflowID)

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"decree_id":    decreeID,
			"publish_mode": "direct",
			"model_selection": map[string]any{
				"global": map[string]any{
					"model": "github-copilot/gpt-5.4-mini",
				},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-model-global-check",
	})
	if runRes.Code != http.StatusBadRequest {
		t.Fatalf("expected run 400 for unavailable global model, got %d (%s)", runRes.Code, runRes.Body.String())
	}
	if !strings.Contains(runRes.Body.String(), "model_selection.global.model") || !strings.Contains(runRes.Body.String(), "github-copilot/gpt-5.4-mini") {
		t.Fatalf("expected global model validation message, got %s", runRes.Body.String())
	}
}

func TestServerCreateRunRejectsUnavailableAgentModelOverride(t *testing.T) {
	t.Parallel()

	mockCatalog := staticProviderModelCatalog{items: []gen.ProviderModel{{
		Id:                    "github-copilot/gpt-5.3-codex",
		ProviderId:            "github-copilot",
		ProviderName:          "GitHub Copilot",
		ModelName:             "GPT-5.3-Codex",
		ThinkingEffortOptions: []gen.ThinkingEffortOption{{Value: "medium", Label: "Medium"}},
	}}}
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ProviderModelCatalog: mockCatalog})
	decreeID := seedDecree(t, handler)
	seedWorkflow(t, handler, workflow.DefaultWorkflowID)

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"decree_id":    decreeID,
			"publish_mode": "direct",
			"model_selection": map[string]any{
				"role_overrides": map[string]any{
					"reviewer-shieldmaiden": map[string]any{
						"model": "github-copilot/gpt-5.4-mini",
					},
				},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-model-role-unavailable",
	})
	if runRes.Code != http.StatusBadRequest {
		t.Fatalf("expected run 400 for unavailable role model, got %d (%s)", runRes.Code, runRes.Body.String())
	}
	if !strings.Contains(runRes.Body.String(), "reviewer-shieldmaiden") || !strings.Contains(runRes.Body.String(), "github-copilot/gpt-5.4-mini") {
		t.Fatalf("expected role/model validation message, got %s", runRes.Body.String())
	}
}

func TestServerCreateRunReturnsServiceUnavailableWhenModelCatalogLookupFails(t *testing.T) {
	t.Parallel()

	mockCatalog := staticProviderModelCatalog{err: &providerModelCatalogUnavailableError{reason: ProviderModelCatalogFailureReasonTimeout, cause: context.DeadlineExceeded}}
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ProviderModelCatalog: mockCatalog})
	decreeID := seedDecree(t, handler)
	seedWorkflow(t, handler, workflow.DefaultWorkflowID)

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"decree_id":    decreeID,
			"publish_mode": "direct",
			"model_selection": map[string]any{
				"global": map[string]any{
					"model": "github-copilot/gpt-5.3-codex",
				},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-model-catalog-failure",
	})
	if runRes.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected run 503 when model catalog fails, got %d (%s)", runRes.Code, runRes.Body.String())
	}
	assertErrorCode(t, runRes.Body.Bytes(), "internal_error")
}

func TestServerCreateRunReturnsWhenRequestContextIsCancelledDuringModelCatalogLookup(t *testing.T) {
	t.Parallel()

	called := make(chan struct{})
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ProviderModelCatalog: blockingProviderModelCatalog{called: called}})
	decreeID := seedDecree(t, handler)
	seedWorkflow(t, handler, workflow.DefaultWorkflowID)

	rawBody, err := json.Marshal(map[string]any{
		"decree_id":    decreeID,
		"publish_mode": "direct",
		"model_selection": map[string]any{
			"global": map[string]any{
				"model": "github-copilot/gpt-5.3-codex",
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal run request: %v", err)
	}

	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, "/api/v1/runs", strings.NewReader(string(rawBody)))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Actor-Id", "actor-1")
	req.Header.Set("X-Role-Id", "chancellor")
	req.Header.Set("Idempotency-Key", "idem-model-cancel")

	res := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(res, req)
	}()

	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for model catalog lookup to start")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after request context cancellation")
	}
}

func TestServerCreateRunPersistsAutonomousApprovalsOverride(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler, withCreateRunBody(map[string]any{"autonomous_approvals": false}))

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected run 200, got %d (%s)", res.Code, res.Body.String())
	}

	var run gen.RunSummary
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run summary: %v", err)
	}
	if run.AutonomousApprovals == nil || *run.AutonomousApprovals {
		t.Fatalf("expected run summary autonomous_approvals=false, got %#v", run.AutonomousApprovals)
	}

	var storedAutonomousApprovals int
	if err := db.QueryRow(`SELECT autonomous_approvals FROM workflow_runs WHERE run_id = ?`, runID).Scan(&storedAutonomousApprovals); err != nil {
		t.Fatalf("query run autonomous approvals: %v", err)
	}
	if storedAutonomousApprovals != 0 {
		t.Fatalf("expected stored autonomous_approvals=0, got %d", storedAutonomousApprovals)
	}
}

func TestServerDecreeMetadataLineageSurvivesCreation(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)
	createRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/decrees",
		body: map[string]any{
			"decree_title": "Planner lineage decree",
			"decree_text":  "Issue from selected planner draft",
			"metadata": map[string]any{
				"planning_session_id":          "planning-session-1",
				"planning_draft_id":            "planning-draft-1",
				"planning_context_snapshot_id": "planning-context-1",
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-decree-lineage",
	})
	if createRes.Code != http.StatusCreated {
		t.Fatalf("expected create decree 201, got %d (%s)", createRes.Code, createRes.Body.String())
	}

	var created gen.CreateDecreeResult
	if err := json.Unmarshal(createRes.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create decree result: %v", err)
	}

	getRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/decrees/" + created.DecreeId,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if getRes.Code != http.StatusOK {
		t.Fatalf("expected get decree 200, got %d (%s)", getRes.Code, getRes.Body.String())
	}

	var decree gen.DecreeDetail
	if err := json.Unmarshal(getRes.Body.Bytes(), &decree); err != nil {
		t.Fatalf("decode decree detail: %v", err)
	}
	if decree.Metadata == nil {
		t.Fatal("expected decree metadata to be present")
	}
	metadata := *decree.Metadata
	if metadata["planning_session_id"] != "planning-session-1" || metadata["planning_draft_id"] != "planning-draft-1" || metadata["planning_context_snapshot_id"] != "planning-context-1" {
		t.Fatalf("expected planner lineage metadata to survive, got %#v", metadata)
	}
}
