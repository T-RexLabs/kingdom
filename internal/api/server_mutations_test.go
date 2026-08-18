package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/workflow"
	"kingdom/internal/state"
)

func TestCreateDecreeRejectsUnknownExternalSnapshotWithoutPersisting(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/decrees", idempotencyKey: "decree-unknown-external-snapshot", body: map[string]any{"decree_title": "Linked", "decree_text": "Text", "external_source_snapshot_id": "does-not-exist"}})
	if response.Code != http.StatusNotFound {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM decrees WHERE decree_title='Linked'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("decree persisted despite missing external snapshot")
	}
}

func TestCreateDecreeRejectsPlanningLineageWithDirectSnapshot(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/decrees", idempotencyKey: "decree-mixed-provenance", body: map[string]any{
		"decree_title": "Mixed", "decree_text": "Text", "external_source_snapshot_id": "snapshot-direct",
		"planning_lineage": map[string]any{"planning_session_id": "session-a", "planning_draft_id": "draft-a"},
	}})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	assertErrorCode(t, response.Body.Bytes(), "policy_violation")
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM decrees WHERE decree_title='Mixed'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("mixed provenance request persisted a decree")
	}
}

func TestCreateDecreeMapsInvalidPlanningLineageToBadRequest(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	seedAPIDecreeLineage(t, db)
	tests := []struct {
		name      string
		lineage   map[string]any
		decreeKey string
	}{
		{name: "cross-session draft", decreeKey: "cross-draft", lineage: map[string]any{"planning_session_id": "session-a", "planning_draft_id": "draft-b"}},
		{name: "formalize missing context binding", decreeKey: "missing-context", lineage: map[string]any{"planning_session_id": "session-a", "planning_draft_id": "draft-unbound"}},
		{name: "failed formalize", decreeKey: "failed-formalize", lineage: map[string]any{"planning_session_id": "session-a", "planning_draft_id": "draft-failed"}},
		{name: "non-formalize", decreeKey: "reply-operation", lineage: map[string]any{"planning_session_id": "session-a", "planning_draft_id": "draft-reply"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/decrees", idempotencyKey: "invalid-lineage-" + test.decreeKey, body: map[string]any{"decree_title": test.decreeKey, "decree_text": "Text", "planning_lineage": test.lineage}})
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			assertErrorCode(t, response.Body.Bytes(), "policy_violation")
		})
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM decrees WHERE decree_title IN ('cross-draft','missing-context','failed-formalize','reply-operation')`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("invalid lineage persisted %d decrees", count)
	}
}

func TestCreateDecreeRejectsClientSuppliedPlanningContextLineage(t *testing.T) {
	handler, _ := newAPIServerForTest(t)
	response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/decrees", idempotencyKey: "client-context-lineage", body: map[string]any{
		"decree_title": "Invalid", "decree_text": "Text",
		"planning_lineage": map[string]any{"planning_session_id": "session-a", "planning_draft_id": "draft-a", "planning_context_snapshot_id": "context-a"},
	}})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestCreateDecreeMapsDuplicateDerivedSourceToConflictAtomically(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	seedAPIDecreeLineage(t, db)
	store := state.NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	if _, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{ConnectionID: "conn-conflict", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-conflict", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-conflict"}); err != nil {
		t.Fatal(err)
	}
	issue := state.NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}
	first, _, err := store.CaptureIssue(ctx, state.CaptureExternalIssueRequest{SourceID: "source-conflict", SnapshotID: "snapshot-conflict-a", ConnectionID: "conn-conflict", ProviderNativeID: "100", RepositoryNativeID: "200", Issue: issue, ProviderUpdatedAt: at, FetchedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	issue.Title = "Later"
	issue.UpdatedAt = "2026-08-12T11:00:00Z"
	second, _, err := store.CaptureIssue(ctx, state.CaptureExternalIssueRequest{SourceID: "ignored-source", SnapshotID: "snapshot-conflict-b", ConnectionID: "conn-conflict", ProviderNativeID: "100", RepositoryNativeID: "200", PredecessorSnapshotID: first.SnapshotID, Issue: issue, ProviderUpdatedAt: issue.UpdatedAt, FetchedAt: issue.UpdatedAt})
	if err != nil {
		t.Fatal(err)
	}
	for _, snapshotID := range []string{first.SnapshotID, second.SnapshotID} {
		mustExecSQL(t, db, `INSERT INTO planning_operation_external_snapshots (operation_id,snapshot_id,assembler_version,context_digest,issue_bytes,comment_bytes,bound_at) VALUES ('operation-good',?,?,?,0,0,?)`, snapshotID, state.ExternalAssemblerVersion, strings.Repeat("0", 64), at)
	}

	response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/decrees", idempotencyKey: "decree-source-conflict", body: map[string]any{"decree_title": "Conflict", "decree_text": "Text", "planning_lineage": map[string]any{"planning_session_id": "session-a", "planning_draft_id": "draft-a"}}})
	if response.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	assertErrorCode(t, response.Body.Bytes(), "idempotency_conflict")
	var decrees, links int
	if err := db.QueryRow(`SELECT COUNT(*) FROM decrees WHERE decree_title='Conflict'`).Scan(&decrees); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_source_links WHERE target_kind='decree'`).Scan(&links); err != nil {
		t.Fatal(err)
	}
	if decrees != 0 || links != 0 {
		t.Fatalf("conflict escaped transaction: decrees=%d links=%d", decrees, links)
	}
}

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
		idempotencyKey: "idem-replay",
	})
	second := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/decrees",
		body: map[string]any{
			"decree_title": "Replay",
			"decree_text":  "Same body",
		},
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
		idempotencyKey: "idem-invalid-workflow",
	})

	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (%s)", res.Code, res.Body.String())
	}
	assertErrorCode(t, res.Body.Bytes(), "policy_violation")
}

func TestServerPublishWorkflowDistinguishesExactAndDifferentExistingDefinitions(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)
	definition := func(nodeID string) map[string]any {
		return map[string]any{
			"version":     "v1",
			"workflow_id": "wf-seed-conflict-v1",
			"nodes": []map[string]any{{
				"node_id": nodeID, "required_role": "council-member", "node_class": "execution_kickoff",
			}},
			"edges": []map[string]any{},
		}
	}
	publish := func(key, nodeID string) *httptest.ResponseRecorder {
		return performJSONRequest(t, handler, requestSpec{
			method: http.MethodPost,
			path:   "/api/v1/workflows/publish",
			body: map[string]any{
				"workflow_id":     "wf-seed-conflict-v1",
				"version":         "seed-v1",
				"definition_json": definition(nodeID),
			},
			idempotencyKey: key,
		})
	}
	if response := publish("seed-first", "kickoff"); response.Code != http.StatusCreated {
		t.Fatalf("first publish status=%d body=%s", response.Code, response.Body.String())
	}
	if response := publish("seed-exact", "kickoff"); response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), `"message":"workflow version already exists"`) {
		t.Fatalf("exact publish status=%d body=%s", response.Code, response.Body.String())
	}
	if response := publish("seed-different", "different"); response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "different definition") {
		t.Fatalf("different publish status=%d body=%s", response.Code, response.Body.String())
	}
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
	authenticateAPITestRequest(req)
	req.Header.Set("Idempotency-Key", testIdempotencyKey(t, "idem-model-cancel", time.Now()))

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
		method: http.MethodGet,
		path:   "/api/v1/runs/" + runID,
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

	handler, db := newAPIServerForTest(t)
	seedAPIDecreeLineage(t, db)
	store := state.NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	if _, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{ConnectionID: "conn-present", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-present", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-present"}); err != nil {
		t.Fatal(err)
	}
	for i, suffix := range []string{"a", "b"} {
		issueNumber := i + 7
		snapshot, _, err := store.CaptureIssue(ctx, state.CaptureExternalIssueRequest{
			SourceID: "source-" + suffix, SnapshotID: "snapshot-" + suffix, ConnectionID: "conn-present", ProviderNativeID: "issue-" + suffix, RepositoryNativeID: "repo-" + suffix,
			Issue:             state.NormalizedExternalIssue{Title: "Issue " + suffix, Body: "body", State: "open", Repository: "owner/repo-" + suffix, IssueNumber: issueNumber, CanonicalURL: fmt.Sprintf("https://github.com/owner/repo-%s/issues/%d", suffix, issueNumber), CreatedAt: at, UpdatedAt: at},
			ProviderUpdatedAt: at, FetchedAt: at,
		})
		if err != nil {
			t.Fatal(err)
		}
		mustExecSQL(t, db, `INSERT INTO planning_operation_external_snapshots (operation_id,snapshot_id,assembler_version,context_digest,issue_bytes,comment_bytes,bound_at) VALUES ('operation-good',?,?,?,0,0,?)`, snapshot.SnapshotID, state.ExternalAssemblerVersion, strings.Repeat("0", 64), at)
	}
	createRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/decrees",
		body: map[string]any{
			"decree_title": "Planner lineage decree",
			"decree_text":  "Issue from selected planner draft",
			"metadata": map[string]any{
				"priority":                     "high",
				"planning_session_id":          "untrusted-session",
				"planning_draft_id":            "untrusted-draft",
				"planning_context_snapshot_id": "untrusted-context",
			},
			"planning_lineage": map[string]any{
				"planning_session_id": "session-a",
				"planning_draft_id":   "draft-a",
			},
		},
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
		method: http.MethodGet,
		path:   "/api/v1/decrees/" + created.DecreeId,
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
	if metadata["priority"] != "high" || metadata["planning_session_id"] != "session-a" || metadata["planning_draft_id"] != "draft-a" || metadata["planning_context_snapshot_id"] != "context-a" {
		t.Fatalf("expected planner lineage metadata to survive, got %#v", metadata)
	}
	if len(decree.ExternalSources) != 2 {
		t.Fatalf("expected both formalize-bound sources, got %#v", decree.ExternalSources)
	}
	snapshots := map[string]bool{}
	for _, source := range decree.ExternalSources {
		snapshots[source.UsedSnapshotId] = true
	}
	if !snapshots["snapshot-a"] || !snapshots["snapshot-b"] {
		t.Fatalf("decree detail hid formalize provenance: %#v", decree.ExternalSources)
	}
	if strings.Contains(getRes.Body.String(), `"external_source":`) {
		t.Fatalf("decree detail retained dead singular provenance: %s", getRes.Body.String())
	}
}

func seedAPIDecreeLineage(t *testing.T, db *sql.DB) {
	t.Helper()
	at := "2026-08-12T10:00:00Z"
	for _, sessionID := range []string{"session-a", "session-b"} {
		mustExecSQL(t, db, `INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES (?,?,'custom','active','ready','/tmp','{}',?,?)`, sessionID, sessionID, at, at)
	}
	for _, draft := range []struct{ id, session string }{{"draft-a", "session-a"}, {"draft-b", "session-b"}, {"draft-failed", "session-a"}, {"draft-reply", "session-a"}, {"draft-unbound", "session-a"}} {
		mustExecSQL(t, db, `INSERT INTO planning_drafts (planning_draft_id,planning_session_id,decree_title,decree_text,summary_json,created_at) VALUES (?,?,?,?,'{}',?)`, draft.id, draft.session, draft.id, "text", at)
	}
	for _, contextSnapshot := range []struct{ id, session string }{{"context-a", "session-a"}, {"context-b", "session-b"}} {
		mustExecSQL(t, db, `INSERT INTO planning_context_snapshots (planning_context_snapshot_id,planning_session_id,summary_markdown,included_files_json,excluded_files_json,created_at) VALUES (?,?,?,'[]','[]',?)`, contextSnapshot.id, contextSnapshot.session, "context", at)
	}
	for _, operation := range []struct{ id, kind, status, result, context string }{{"operation-good", "formalize", "succeeded", "draft-a", "context-a"}, {"operation-failed", "formalize", "failed", "draft-failed", "context-a"}, {"operation-reply", "reply", "succeeded", "draft-reply", "context-a"}, {"operation-unbound", "formalize", "succeeded", "draft-unbound", ""}} {
		mustExecSQL(t, db, `INSERT INTO planning_operations (operation_id,acp_run_id,planning_session_id,planning_context_snapshot_id,type,status,request_payload_json,lease_owner,lease_version,observed_output_text,result_ref,created_at) VALUES (?,?, 'session-a',NULLIF(?,''),?,?, '{}','',0,'',?,?)`, operation.id, operation.id, operation.context, operation.kind, operation.status, operation.result, at)
	}
}
