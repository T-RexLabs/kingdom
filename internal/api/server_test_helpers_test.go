package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/workflow"
	"kingdom/internal/dbmigrations"
	"kingdom/internal/planning"
	"kingdom/internal/state"
	"kingdom/internal/testutil/planningfixture"
)

type testPlannerAgent struct{}

func apiTestReceiptMetadata(input planning.AgentContext, finalText string) (planning.ProviderExecutionMetadata, error) {
	metadata := planning.ProviderExecutionMetadata{FinalText: finalText, OutcomeState: "success", CompletionStructuredPayloadJSON: "{}"}
	if input.ProviderInvocation == nil || input.ProviderInvocation.OnCompletionReceipt == nil {
		return metadata, nil
	}
	accepted, err := input.ProviderInvocation.OnCompletionReceipt(planning.CompletionReceipt{
		ProtocolVersion:       "v1",
		AttemptID:             input.ProviderInvocation.AttemptID,
		InvocationID:          input.ProviderInvocation.InvocationID,
		CompletionCapability:  input.ProviderInvocation.CompletionCapability,
		FinalText:             finalText,
		StructuredPayloadJSON: "{}",
	})
	if err != nil {
		return planning.ProviderExecutionMetadata{}, err
	}
	metadata.CompletionReceiptEvidenceID = accepted.EvidenceID
	metadata.CompletionStructuredPayloadJSON = accepted.StructuredPayloadJSON
	if metadata.CompletionStructuredPayloadJSON == "" {
		metadata.CompletionStructuredPayloadJSON = "{}"
	}
	return metadata, nil
}

var defaultPlannerAPITestScript = planningfixture.DefaultAPIScript()

type testStreamingPlannerAgent struct {
	chunks   []string
	question *planning.StructuredQuestion
	final    string
}

func (a *testPlannerAgent) OpeningTurn(_ context.Context, input planning.AgentContext) (planning.AgentTurn, error) {
	return planning.AgentTurn{
		ContentMarkdown: defaultPlannerAPITestScript.OpeningContent,
		Question: &planning.StructuredQuestion{
			Prompt: defaultPlannerAPITestScript.OpeningQuestion.Prompt,
			Type:   defaultPlannerAPITestScript.OpeningQuestion.Type,
		},
	}, nil
}

func (a *testPlannerAgent) OpeningTurnStream(_ context.Context, input planning.AgentContext, onChunk func(text string)) (planning.AgentTurn, error) {
	turn, err := a.OpeningTurn(context.Background(), input)
	if err == nil && onChunk != nil {
		onChunk(turn.ContentMarkdown)
	}
	return turn, err
}

func (a *testPlannerAgent) ContinueTurnStream(_ context.Context, input planning.AgentContext, onChunk func(text string)) (planning.AgentTurn, error) {
	metadata, err := apiTestReceiptMetadata(input, defaultPlannerAPITestScript.ContinueContent)
	if err != nil {
		return planning.AgentTurn{}, err
	}
	return planning.AgentTurn{
		ContentMarkdown: defaultPlannerAPITestScript.ContinueContent,
		Question: &planning.StructuredQuestion{
			Prompt: defaultPlannerAPITestScript.ContinueQuestion.Prompt,
			Type:   defaultPlannerAPITestScript.ContinueQuestion.Type,
		},
		Metadata: metadata,
	}, nil
}

func (a *testPlannerAgent) FormalizePlanStream(_ context.Context, input planning.AgentContext, onChunk func(text string)) (planning.FormalizedPlan, error) {
	metadata, err := apiTestReceiptMetadata(input, defaultPlannerAPITestScript.FormalizedText)
	if err != nil {
		return planning.FormalizedPlan{}, err
	}
	return planning.FormalizedPlan{
		DecreeTitle: input.Title,
		DecreeText:  defaultPlannerAPITestScript.FormalizedText,
		Metadata:    metadata,
	}, nil
}

func (a *testPlannerAgent) GenerateTitle(_ context.Context, input planning.AgentContext) (planning.GeneratedTitle, error) {
	metadata, err := apiTestReceiptMetadata(input, defaultPlannerAPITestScript.GeneratedTitle)
	if err != nil {
		return planning.GeneratedTitle{}, err
	}
	return planning.GeneratedTitle{
		Title:    defaultPlannerAPITestScript.GeneratedTitle,
		Metadata: metadata,
	}, nil
}

func (a *testStreamingPlannerAgent) OpeningTurn(_ context.Context, input planning.AgentContext) (planning.AgentTurn, error) {
	return (&testPlannerAgent{}).OpeningTurn(context.Background(), input)
}

func (a *testStreamingPlannerAgent) OpeningTurnStream(_ context.Context, input planning.AgentContext, onChunk func(text string)) (planning.AgentTurn, error) {
	return (&testPlannerAgent{}).OpeningTurnStream(context.Background(), input, onChunk)
}

func (a *testStreamingPlannerAgent) ContinueTurnStream(_ context.Context, input planning.AgentContext, onChunk func(text string)) (planning.AgentTurn, error) {
	for _, chunk := range a.chunks {
		if onChunk != nil {
			onChunk(chunk)
		}
	}
	metadata, err := apiTestReceiptMetadata(input, a.final)
	if err != nil {
		return planning.AgentTurn{}, err
	}
	return planning.AgentTurn{
		ContentMarkdown: a.final,
		Question:        a.question,
		Metadata:        metadata,
	}, nil
}

func (a *testStreamingPlannerAgent) FormalizePlanStream(_ context.Context, input planning.AgentContext, onChunk func(text string)) (planning.FormalizedPlan, error) {
	return (&testPlannerAgent{}).FormalizePlanStream(context.Background(), input, onChunk)
}

func (a *testStreamingPlannerAgent) GenerateTitle(_ context.Context, input planning.AgentContext) (planning.GeneratedTitle, error) {
	return (&testPlannerAgent{}).GenerateTitle(context.Background(), input)
}

type requestSpec struct {
	method         string
	path           string
	body           map[string]any
	actorID        string
	roleID         string
	idempotencyKey string
}

func performJSONRequest(t *testing.T, handler http.Handler, spec requestSpec) *httptest.ResponseRecorder {
	t.Helper()

	var body io.Reader
	if spec.body != nil {
		raw, err := json.Marshal(spec.body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		body = bytes.NewReader(raw)
	}

	req := httptest.NewRequest(spec.method, spec.path, body)
	if spec.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if spec.actorID != "" {
		req.Header.Set("X-Actor-Id", spec.actorID)
	}
	if spec.roleID != "" {
		req.Header.Set("X-Role-Id", spec.roleID)
	}
	if spec.idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", spec.idempotencyKey)
	}

	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	return res
}

func newAPIServerForTest(t *testing.T) (http.Handler, *sql.DB) {
	t.Helper()
	return newAPIServerForTestWithConfig(t, ServerConfig{})
}

func newAPIServerForTestWithConfig(t *testing.T, cfg ServerConfig) (http.Handler, *sql.DB) {
	t.Helper()

	db := cfg.DB
	if db == nil {
		db = openAPIServerTestDB(t)
	}

	cfg.DB = db
	if strings.TrimSpace(cfg.ProjectRoot) == "" {
		cfg.ProjectRoot = t.TempDir()
	}
	if strings.TrimSpace(cfg.ProviderEngine) == "" {
		cfg.ProviderEngine = "opencode"
	}
	if cfg.ProviderModelCatalog == nil {
		cfg.ProviderModelCatalog = staticProviderModelCatalog{items: defaultTestProviderModels()}
	}
	if cfg.PlannerAgent == nil {
		cfg.PlannerAgent = &testPlannerAgent{}
	}
	handler, err := NewHandler(cfg)
	if err != nil {
		t.Fatalf("build api handler: %v", err)
	}

	return handler, db
}

func defaultTestProviderModels() []gen.ProviderModel {
	return []gen.ProviderModel{
		{
			Id:                    "github-copilot/gpt-4.1",
			ProviderId:            "github-copilot",
			ProviderName:          "GitHub Copilot",
			ModelName:             "GPT-4.1",
			ThinkingEffortOptions: []gen.ThinkingEffortOption{{Value: "low", Label: "Low"}},
		},
		{
			Id:                    "github-copilot/gpt-5.3-codex",
			ProviderId:            "github-copilot",
			ProviderName:          "GitHub Copilot",
			ModelName:             "GPT-5.3-Codex",
			ThinkingEffortOptions: []gen.ThinkingEffortOption{{Value: "low", Label: "Low"}},
		},
	}
}

func defaultTestRunModelSelectionBody() map[string]any {
	return map[string]any{
		"global": map[string]any{
			"model": defaultTestProviderModels()[0].Id,
		},
	}
}

func openAPIServerTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable foreign keys pragma: %v", err)
	}

	names, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	for _, name := range names {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		if _, err := db.Exec(string(raw)); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}

	seedRoles(t, db)
	return db
}

type staticProviderModelCatalog struct {
	items          []gen.ProviderModel
	err            error
	status         ProviderModelCatalogStatus
	failureReason  ProviderModelCatalogFailureReason
	failureMessage string
}

type blockingProviderModelCatalog struct {
	called chan struct{}
}

func refString(value string) *string {
	return &value
}

func firstNonEmptyAPITest(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func repoRootForAPITests(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve caller")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

func (s staticProviderModelCatalog) List(_ context.Context, _ bool) ([]gen.ProviderModel, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.status == ProviderModelCatalogStatusUnavailable {
		return nil, &providerModelCatalogUnavailableError{reason: s.failureReason, cause: errors.New(firstNonEmptyAPITest(s.failureMessage, "provider model catalog unavailable"))}
	}
	rows := make([]gen.ProviderModel, len(s.items))
	copy(rows, s.items)
	return rows, nil
}

func (s staticProviderModelCatalog) Snapshot(_ context.Context, _ bool) (ProviderModelCatalogSnapshot, error) {
	if s.err != nil {
		return ProviderModelCatalogSnapshot{}, s.err
	}
	rows := make([]gen.ProviderModel, len(s.items))
	copy(rows, s.items)
	status := s.status
	if status == "" {
		status = ProviderModelCatalogStatusFresh
	}
	snapshot := ProviderModelCatalogSnapshot{Items: rows, Status: status}
	if s.failureReason != ProviderModelCatalogFailureReasonNone {
		snapshot.FailureReason = s.failureReason
	}
	if strings.TrimSpace(s.failureMessage) != "" {
		snapshot.FailureMessage = s.failureMessage
	}
	if status == ProviderModelCatalogStatusUnavailable {
		snapshot.UnavailableErr = &providerModelCatalogUnavailableError{reason: s.failureReason, cause: errors.New(firstNonEmptyAPITest(s.failureMessage, "provider model catalog unavailable"))}
	}
	return snapshot, nil
}

func (c blockingProviderModelCatalog) List(ctx context.Context, _ bool) ([]gen.ProviderModel, error) {
	if c.called != nil {
		close(c.called)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (c blockingProviderModelCatalog) Snapshot(ctx context.Context, _ bool) (ProviderModelCatalogSnapshot, error) {
	if c.called != nil {
		close(c.called)
	}
	<-ctx.Done()
	return ProviderModelCatalogSnapshot{}, ctx.Err()
}

func seedRoles(t *testing.T, db *sql.DB) {
	t.Helper()

	roles := []string{
		"king",
		"allfather",
		"chancellor",
		"council-member",
		"noble-delegate",
		"clergy-delegate",
		"burgher-delegate",
		"peasant-delegate",
		"builder-berserker",
		"builder",
		"reviewer-shieldmaiden",
		"intelligence-qa-scout",
		"intelligence-nightraven",
		"intelligence-loki-chaos",
		"intelligence",
	}
	now := time.Now().UTC().Format(time.RFC3339)

	for _, roleID := range roles {
		if _, err := db.Exec(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES (?, ?, 1, ?)`, roleID, roleID, now); err != nil {
			t.Fatalf("seed role %s: %v", roleID, err)
		}
	}
}

type seedRunOption func(body map[string]any)

func withCreateRunBody(extra map[string]any) seedRunOption {
	return func(body map[string]any) {
		for key, value := range extra {
			body[key] = value
		}
	}
}

func seedRunViaAPI(t *testing.T, handler http.Handler, opts ...seedRunOption) string {
	t.Helper()

	decreeRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/decrees",
		body: map[string]any{
			"decree_title": "Seed decree",
			"decree_text":  "Seed text",
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-seed-decree",
	})
	if decreeRes.Code != http.StatusCreated {
		t.Fatalf("seed decree expected 201, got %d (%s)", decreeRes.Code, decreeRes.Body.String())
	}

	var decree gen.CreateDecreeResult
	if err := json.Unmarshal(decreeRes.Body.Bytes(), &decree); err != nil {
		t.Fatalf("decode decree response: %v", err)
	}

	publishRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": workflow.DefaultWorkflowID,
			"version":     "seed-run-v1",
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": workflow.DefaultWorkflowID,
				"nodes":       []map[string]any{{"node_id": "kickoff", "required_role": "council-member", "node_class": "execution_kickoff"}},
				"edges":       []map[string]any{},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-seed-workflow",
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("seed workflow expected 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
	}

	runBody := map[string]any{
		"decree_id":       decree.DecreeId,
		"publish_mode":    "direct",
		"model_selection": defaultTestRunModelSelectionBody(),
	}
	for _, apply := range opts {
		if apply != nil {
			apply(runBody)
		}
	}

	runRes := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs",
		body:           runBody,
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-seed-run",
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("seed run expected 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}

	var run gen.RunSummary
	if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run response: %v", err)
	}

	return run.RunId
}

func seedDecree(t *testing.T, handler http.Handler) string {
	t.Helper()
	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/decrees",
		body: map[string]any{
			"decree_title": "Seed decree",
			"decree_text":  "Seed text",
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-seed-decree",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("seed decree expected 201, got %d (%s)", res.Code, res.Body.String())
	}
	var decree gen.CreateDecreeResult
	if err := json.Unmarshal(res.Body.Bytes(), &decree); err != nil {
		t.Fatalf("decode decree seed: %v", err)
	}
	return decree.DecreeId
}

func seedWorkflow(t *testing.T, handler http.Handler, workflowID string) {
	t.Helper()
	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": workflowID,
			"version":     workflowID + "-v1",
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": workflowID,
				"nodes": []map[string]any{map[string]any{
					"node_id":       "kickoff",
					"required_role": "council-member",
					"node_class":    "execution_kickoff",
				}},
				"edges": []map[string]any{},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-seed-workflow-" + workflowID,
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("seed workflow expected 201, got %d (%s)", res.Code, res.Body.String())
	}
}

func seedRunWithLinearExecutionWorkflow(t *testing.T, handler http.Handler) string {
	t.Helper()

	decreeRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/decrees",
		body: map[string]any{
			"decree_title": "Linear replay decree",
			"decree_text":  "Linear replay text",
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-linear-decree",
	})
	if decreeRes.Code != http.StatusCreated {
		t.Fatalf("linear decree expected 201, got %d (%s)", decreeRes.Code, decreeRes.Body.String())
	}

	var decree gen.CreateDecreeResult
	if err := json.Unmarshal(decreeRes.Body.Bytes(), &decree); err != nil {
		t.Fatalf("decode linear decree response: %v", err)
	}

	publishRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": workflow.DefaultWorkflowID,
			"version":     "linear-replay-v1",
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": workflow.DefaultWorkflowID,
				"nodes": []map[string]any{
					{"node_id": "plan_drafting", "required_role": "council-member", "node_class": "plan_drafting"},
					{"node_id": "execution_kickoff", "required_role": "council-member", "node_class": "execution_kickoff"},
					{"node_id": "delegated_execution_berserker", "required_role": "builder-berserker", "node_class": "delegated_execution"},
					{"node_id": "execution_review_shieldmaiden", "required_role": "reviewer-shieldmaiden", "node_class": "execution_review"},
					{"node_id": "validation_gates_qa_scout", "required_role": "intelligence-qa-scout", "node_class": "validation_gates"},
					{"node_id": "royal_closure", "required_role": "king", "node_class": "royal_closure"},
				},
				"edges": []map[string]any{
					{"from": "plan_drafting", "to": "execution_kickoff", "on": "success"},
					{"from": "execution_kickoff", "to": "delegated_execution_berserker", "on": "success"},
					{"from": "delegated_execution_berserker", "to": "execution_review_shieldmaiden", "on": "success"},
					{"from": "execution_review_shieldmaiden", "to": "validation_gates_qa_scout", "on": "success"},
					{"from": "validation_gates_qa_scout", "to": "royal_closure", "on": "success"},
				},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-linear-workflow",
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("linear workflow expected 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
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
		idempotencyKey: "idem-linear-run",
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("linear run expected 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}

	var run gen.RunSummary
	if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode linear run response: %v", err)
	}
	return run.RunId
}

func seedRunWithCustomPlanDraftingWorkflow(t *testing.T, handler http.Handler, planNodeID string) string {
	t.Helper()
	trimmedPlanNodeID := strings.TrimSpace(planNodeID)
	if trimmedPlanNodeID == "" {
		t.Fatal("plan node id is required")
	}

	decreeRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/decrees",
		body: map[string]any{
			"decree_title": "Custom plan replay decree",
			"decree_text":  "Custom plan replay text",
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-custom-plan-decree-" + trimmedPlanNodeID,
	})
	if decreeRes.Code != http.StatusCreated {
		t.Fatalf("custom plan decree expected 201, got %d (%s)", decreeRes.Code, decreeRes.Body.String())
	}

	var decree gen.CreateDecreeResult
	if err := json.Unmarshal(decreeRes.Body.Bytes(), &decree); err != nil {
		t.Fatalf("decode custom plan decree response: %v", err)
	}

	publishRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": workflow.DefaultWorkflowID,
			"version":     "custom-plan-replay-" + trimmedPlanNodeID,
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": workflow.DefaultWorkflowID,
				"nodes": []map[string]any{
					{"node_id": trimmedPlanNodeID, "required_role": "council-member", "node_class": "plan_drafting"},
					{"node_id": "execution_kickoff", "required_role": "council-member", "node_class": "execution_kickoff"},
					{"node_id": "delegated_execution_berserker", "required_role": "builder-berserker", "node_class": "delegated_execution"},
					{"node_id": "royal_closure", "required_role": "king", "node_class": "royal_closure"},
				},
				"edges": []map[string]any{
					{"from": trimmedPlanNodeID, "to": "execution_kickoff", "on": "success"},
					{"from": "execution_kickoff", "to": "delegated_execution_berserker", "on": "success"},
					{"from": "delegated_execution_berserker", "to": "royal_closure", "on": "success"},
				},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-custom-plan-workflow-" + trimmedPlanNodeID,
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("custom plan workflow expected 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
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
		idempotencyKey: "idem-custom-plan-run-" + trimmedPlanNodeID,
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("custom plan run expected 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}

	var run gen.RunSummary
	if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode custom plan run response: %v", err)
	}
	return run.RunId
}

func seedRunWithMultiPlanDraftingWorkflow(t *testing.T, handler http.Handler) string {
	t.Helper()

	decreeRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/decrees",
		body: map[string]any{
			"decree_title": "Multi plan decree",
			"decree_text":  "Multi plan decree text",
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-multi-plan-decree",
	})
	if decreeRes.Code != http.StatusCreated {
		t.Fatalf("multi plan decree expected 201, got %d (%s)", decreeRes.Code, decreeRes.Body.String())
	}

	var decree gen.CreateDecreeResult
	if err := json.Unmarshal(decreeRes.Body.Bytes(), &decree); err != nil {
		t.Fatalf("decode multi plan decree response: %v", err)
	}

	publishRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": workflow.DefaultWorkflowID,
			"version":     "multi-plan-v1",
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": workflow.DefaultWorkflowID,
				"nodes": []map[string]any{
					{"node_id": "advisory_synthesis", "required_role": "chancellor", "node_class": "advisory_synthesis"},
					{"node_id": "plan_drafting_scope", "required_role": "council-member", "node_class": "plan_drafting"},
					{"node_id": "plan_drafting_delivery", "required_role": "council-member", "node_class": "plan_drafting"},
					{"node_id": "king_plan_decision", "required_role": "king", "node_class": "king_plan_decision"},
					{"node_id": "execution_kickoff", "required_role": "council-member", "node_class": "execution_kickoff"},
				},
				"edges": []map[string]any{
					{"from": "advisory_synthesis", "to": "plan_drafting_scope", "on": "success"},
					{"from": "advisory_synthesis", "to": "plan_drafting_delivery", "on": "success"},
					{"from": "plan_drafting_scope", "to": "king_plan_decision", "on": "success"},
					{"from": "plan_drafting_delivery", "to": "king_plan_decision", "on": "success"},
					{"from": "king_plan_decision", "to": "execution_kickoff", "on": "approved"},
				},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-multi-plan-workflow",
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("multi plan workflow expected 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
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
		idempotencyKey: "idem-multi-plan-run",
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("multi plan run expected 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}

	var run gen.RunSummary
	if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode multi plan run response: %v", err)
	}
	return run.RunId
}

func seedRunWithGovernanceBranchWorkflow(t *testing.T, handler http.Handler) string {
	t.Helper()

	decreeRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/decrees",
		body: map[string]any{
			"decree_title": "Governance branch decree",
			"decree_text":  "Governance branch text",
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-governance-branch-decree",
	})
	if decreeRes.Code != http.StatusCreated {
		t.Fatalf("governance branch decree expected 201, got %d (%s)", decreeRes.Code, decreeRes.Body.String())
	}

	var decree gen.CreateDecreeResult
	if err := json.Unmarshal(decreeRes.Body.Bytes(), &decree); err != nil {
		t.Fatalf("decode governance branch decree response: %v", err)
	}

	publishRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": workflow.DefaultWorkflowID,
			"version":     "governance-branch-v1",
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": workflow.DefaultWorkflowID,
				"nodes": []map[string]any{
					{"node_id": "plan_drafting", "required_role": "council-member", "node_class": "plan_drafting"},
					{"node_id": "king_plan_decision", "required_role": "king", "node_class": "king_plan_decision"},
					{"node_id": "execution_kickoff", "required_role": "council-member", "node_class": "execution_kickoff"},
					{"node_id": "execution_review_shieldmaiden", "required_role": "reviewer-shieldmaiden", "node_class": "execution_review"},
					{"node_id": "delegated_execution_berserker", "required_role": "builder-berserker", "node_class": "delegated_execution"},
					{"node_id": "royal_closure", "required_role": "king", "node_class": "royal_closure"},
				},
				"edges": []map[string]any{
					{"from": "plan_drafting", "to": "king_plan_decision", "on": "success"},
					{"from": "king_plan_decision", "to": "execution_kickoff", "on": "approved"},
					{"from": "king_plan_decision", "to": "execution_review_shieldmaiden", "on": "needs_rework"},
					{"from": "execution_kickoff", "to": "delegated_execution_berserker", "on": "success"},
					{"from": "delegated_execution_berserker", "to": "royal_closure", "on": "success"},
				},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-governance-branch-workflow",
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("governance branch workflow expected 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
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
		idempotencyKey: "idem-governance-branch-run",
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("governance branch run expected 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}

	var run gen.RunSummary
	if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode governance branch run response: %v", err)
	}
	return run.RunId
}

func seedRunWithPlanningWaveWorkflow(t *testing.T, handler http.Handler, opts ...seedRunOption) string {
	t.Helper()

	decreeRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/decrees",
		body: map[string]any{
			"decree_title": "Planning wave decree",
			"decree_text":  "Planning wave text",
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-planning-wave-decree",
	})
	if decreeRes.Code != http.StatusCreated {
		t.Fatalf("planning wave decree expected 201, got %d (%s)", decreeRes.Code, decreeRes.Body.String())
	}

	var decree gen.CreateDecreeResult
	if err := json.Unmarshal(decreeRes.Body.Bytes(), &decree); err != nil {
		t.Fatalf("decode planning wave decree response: %v", err)
	}

	publishRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": workflow.DefaultWorkflowID,
			"version":     "planning-wave-v1",
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": workflow.DefaultWorkflowID,
				"nodes": []map[string]any{
					{"node_id": "advisory_synthesis", "required_role": "chancellor", "node_class": "advisory_synthesis"},
					{"node_id": "plan_drafting", "required_role": "council-member", "node_class": "plan_drafting"},
					{"node_id": "king_plan_decision", "required_role": "king", "node_class": "king_plan_decision"},
					{"node_id": "execution_kickoff", "required_role": "council-member", "node_class": "execution_kickoff"},
				},
				"edges": []map[string]any{
					{"from": "advisory_synthesis", "to": "plan_drafting", "on": "success"},
					{"from": "plan_drafting", "to": "king_plan_decision", "on": "success"},
					{"from": "king_plan_decision", "to": "execution_kickoff", "on": "approved"},
				},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-planning-wave-workflow",
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("planning wave workflow expected 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
	}

	runBody := map[string]any{
		"decree_id":       decree.DecreeId,
		"publish_mode":    "direct",
		"model_selection": defaultTestRunModelSelectionBody(),
	}
	for _, apply := range opts {
		if apply != nil {
			apply(runBody)
		}
	}

	runRes := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs",
		body:           runBody,
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-planning-wave-run",
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("planning wave run expected 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}

	var run gen.RunSummary
	if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode planning wave run response: %v", err)
	}
	return run.RunId
}

func seedTimelineEvents(t *testing.T, db *sql.DB, runID string) {
	t.Helper()

	ingestor := state.NewAttemptEventIngestor(db, nil)
	now := time.Now().UTC().Format(time.RFC3339)

	for i := 1; i <= 3; i++ {
		payload, _ := json.Marshal(map[string]any{"step": i})
		if _, err := ingestor.Ingest(context.Background(), state.AttemptEventIngestionRequest{
			EventID:         "evt-seed-" + strconv.Itoa(i),
			RunID:           runID,
			NodeRunID:       sql.NullString{},
			AttemptID:       "attempt-seed",
			SchemaVersion:   "v1",
			EventType:       "attempt.progress",
			Seq:             i,
			PayloadJSON:     string(payload),
			ActorID:         "actor-1",
			RoleID:          "chancellor",
			AuthoritySource: "test.seed",
			ReasonCode:      "internal_error",
			ReasonText:      "seed",
			CorrelationID:   "corr-seed",
			CausationID:     "caus-seed",
			CreatedAt:       now,
		}); err != nil {
			t.Fatalf("seed timeline event %d: %v", i, err)
		}
	}
}

func seedArtifact(t *testing.T, db *sql.DB, runID string) {
	t.Helper()

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-1', 'council-member', 'idle', NULL, 0, ?, ?)`, now, now); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('node-run-1', ?, 'kickoff', 1,1, 'pending', 'council-member', 'agent-1', 1, NULL, NULL)`, runID); err != nil {
		t.Fatalf("seed node run: %v", err)
	}
	workspaceRoot := t.TempDir()
	reportPath := filepath.Join(workspaceRoot, "artifacts", "report.txt")
	if err := os.MkdirAll(filepath.Dir(reportPath), 0o755); err != nil {
		t.Fatalf("create artifact directory: %v", err)
	}
	if err := os.WriteFile(reportPath, []byte("seed artifact"), 0o644); err != nil {
		t.Fatalf("write artifact file: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO workspaces (workspace_id, run_id, node_run_id, workspace_path, status, created_at, released_at) VALUES ('ws-artifact-1', ?, 'node-run-1', ?, 'released', ?, ?)`, runID, workspaceRoot, now, now); err != nil {
		t.Fatalf("seed workspace: %v", err)
	}
	body := []byte("seed artifact")
	sum := sha256.Sum256(body)
	checksum := hex.EncodeToString(sum[:])
	if _, err := db.Exec(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact-1', ?, 'node-run-1', 'attempt-seed', 'deliverable', 'report.txt', 'text/plain', 'artifacts/report.txt', ?, 13, 'council-member', ?)`, runID, checksum, now); err != nil {
		t.Fatalf("seed artifact: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO artifact_contents (artifact_id, content, sha256, size_bytes, created_at) VALUES ('artifact-1', ?, ?, 13, ?)`, body, checksum, now); err != nil {
		t.Fatalf("seed artifact content: %v", err)
	}
}

func seedReadyPlanReworkNodes(t *testing.T, db *sql.DB, runID string) string {
	t.Helper()

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`UPDATE workflow_runs SET plan_state = 'awaiting_king_decision' WHERE run_id = ?`, runID); err != nil {
		t.Fatalf("set run plan_state awaiting_king_decision: %v", err)
	}

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'chancellor', 'idle', NULL, 0, ?, ?)`, "agent_chancellor_"+runID, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'council-member', 'idle', NULL, 0, ?, ?)`, "agent_council_"+runID, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'king', 'idle', NULL, 0, ?, ?)`, "agent_king_"+runID, now, now)

	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'advisory_synthesis', 1,1, 'succeeded', 'chancellor', ?, 1, ?, ?)`, "nr_plan_rework_advisory_"+runID, runID, "agent_chancellor_"+runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'plan_drafting', 1,1, 'succeeded', 'council-member', ?, 1, ?, ?)`, "nr_plan_rework_plan_"+runID, runID, "agent_council_"+runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'king_plan_decision', 1,1, 'pending', 'king', ?, 1, NULL, NULL)`, "nr_plan_rework_king_"+runID, runID, "agent_king_"+runID)

	messageID := "msg_plan_current_" + runID
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES (?, ?, ?, ?, ?, 'status_update', 'normal', ?, ?)`,
		messageID,
		runID,
		"agent_council_"+runID,
		"agent_king_"+runID,
		"thread_"+runID+"_plan",
		`{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Current council plan revision"}`,
		now,
	)

	planningOutputID := "planrev_current_" + runID
	seedAttemptExecution(t, db, runID+":plan_drafting:1", runID, "nr_plan_rework_plan_"+runID, now)
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES (?, ?, 'plan_drafting', ?, ?, ?, ?, 'plan_revision', ?, ?)`,
		planningOutputID,
		runID,
		"nr_plan_rework_plan_"+runID,
		runID+":plan_drafting:1",
		"agent_council_"+runID,
		messageID,
		"Current council plan revision",
		now,
	)

	revision := mustRefreshRunPlanRevision(t, db, runID)
	return revision.PlanRevisionID
}

func seedCurrentPlanRevisionForNode(t *testing.T, db *sql.DB, runID, nodeID, content string) string {
	t.Helper()
	seedPlanOutputForNode(t, db, runID, nodeID, content)
	revision := mustRefreshRunPlanRevision(t, db, runID)
	return revision.PlanRevisionID
}

func seedPlanOutputForNode(t *testing.T, db *sql.DB, runID, nodeID, content string) string {
	t.Helper()

	trimmedNodeID := strings.TrimSpace(nodeID)
	if trimmedNodeID == "" {
		t.Fatal("plan node id is required")
	}
	trimmedContent := strings.TrimSpace(content)
	if trimmedContent == "" {
		trimmedContent = "Current council plan revision"
	}
	now := time.Now().UTC()
	nowRFC3339 := now.Format(time.RFC3339)
	suffix := strconv.FormatInt(now.UnixNano(), 10)
	councilAgentID := "agent_council_" + trimmedNodeID + "_" + runID
	kingAgentID := "agent_king_" + trimmedNodeID + "_" + runID
	messageID := "msg_plan_" + trimmedNodeID + "_" + runID + "_" + suffix
	nodeRunID := "nr_plan_" + trimmedNodeID + "_" + runID
	attemptID := runID + ":" + trimmedNodeID + ":" + suffix
	planningOutputID := "planout_" + trimmedNodeID + "_" + runID + "_" + suffix
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'council-member', 'idle', NULL, 0, ?, ?)`, councilAgentID, nowRFC3339, nowRFC3339)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'king', 'idle', NULL, 0, ?, ?)`, kingAgentID, nowRFC3339, nowRFC3339)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, ?, 1,1, 'succeeded', 'council-member', ?, 1, ?, ?)`, nodeRunID, runID, trimmedNodeID, councilAgentID, nowRFC3339, nowRFC3339)
	seedAttemptExecution(t, db, attemptID, runID, nodeRunID, nowRFC3339)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES (?, ?, ?, ?, ?, 'status_update', 'normal', ?, ?)`,
		messageID,
		runID,
		councilAgentID,
		kingAgentID,
		"thread_"+runID+"_"+trimmedNodeID,
		fmt.Sprintf(`{"message_source":"communication","communication_type":"response_received","node_id":%q,"response_full":%q}`,
			trimmedNodeID,
			trimmedContent,
		),
		nowRFC3339,
	)
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'plan_revision', ?, ?)`,
		planningOutputID,
		runID,
		trimmedNodeID,
		nodeRunID,
		attemptID,
		councilAgentID,
		messageID,
		trimmedContent,
		nowRFC3339,
	)
	return planningOutputID
}

func seedAttemptExecution(t *testing.T, db *sql.DB, attemptID, runID, nodeRunID, occurredAt string) {
	t.Helper()
	mustExecSQL(t, db, `INSERT OR IGNORE INTO attempt_executions (attempt_id, run_id, node_run_id, state, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES (?, ?, ?, 'committed_success', '', NULL, 0, '', ?, ?, ?, '', '', ?, ?)`,
		attemptID,
		runID,
		nodeRunID,
		occurredAt,
		occurredAt,
		occurredAt,
		occurredAt,
		occurredAt,
	)
}

func mustRefreshRunPlanRevision(t *testing.T, db *sql.DB, runID string) state.RunPlanRevision {
	t.Helper()
	revision, ok, err := state.NewRunPlanRevisionStore(db).RefreshCurrentRunPlanRevision(context.Background(), runID)
	if err != nil {
		t.Fatalf("refresh run plan revision for %q: %v", runID, err)
	}
	if !ok {
		t.Fatalf("expected run %q to have a current run plan revision", runID)
	}
	return revision
}

func seedPlanningWaveAttempts(t *testing.T, db *sql.DB, runID string, maxAttempt int) {
	t.Helper()
	if maxAttempt <= 1 {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339)
	for attempt := 2; attempt <= maxAttempt; attempt++ {
		mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'advisory_synthesis', ?, ?, 'succeeded', 'chancellor', ?, 1, ?, ?)`, fmt.Sprintf("nr_plan_rework_advisory_%d_%s", attempt, runID), runID, attempt, attempt, "agent_chancellor_"+runID, now, now)
		mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'plan_drafting', ?, ?, 'succeeded', 'council-member', ?, 1, ?, ?)`, fmt.Sprintf("nr_plan_rework_plan_%d_%s", attempt, runID), runID, attempt, attempt, "agent_council_"+runID, now, now)
		attemptID := fmt.Sprintf("%s:plan_drafting:%d", runID, attempt)
		nodeRunID := fmt.Sprintf("nr_plan_rework_plan_%d_%s", attempt, runID)
		seedAttemptExecution(t, db, attemptID, runID, nodeRunID, now)
		mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES (?, ?, 'plan_drafting', ?, ?, ?, '', 'plan_revision', ?, ?)`,
			fmt.Sprintf("planrev_planning_wave_%d_%s", attempt, runID),
			runID,
			nodeRunID,
			attemptID,
			"agent_council_"+runID,
			fmt.Sprintf("Current council plan revision attempt %d", attempt),
			now,
		)
	}
	_ = mustRefreshRunPlanRevision(t, db, runID)
}

func seedLinearGovernanceAttempts(t *testing.T, db *sql.DB, runID string, maxAttempt int) {
	t.Helper()
	if maxAttempt <= 0 {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339)
	rolesByNode := map[string]string{
		"plan_drafting":                 "council-member",
		"execution_kickoff":             "council-member",
		"delegated_execution_berserker": "builder-berserker",
		"execution_review_shieldmaiden": "reviewer-shieldmaiden",
		"validation_gates_qa_scout":     "intelligence-qa-scout",
		"royal_closure":                 "king",
	}
	agentsByRole := map[string]string{
		"council-member":        "agent_seed_council_" + runID,
		"builder-berserker":     "agent_seed_builder_" + runID,
		"reviewer-shieldmaiden": "agent_seed_reviewer_" + runID,
		"intelligence-qa-scout": "agent_seed_scout_" + runID,
		"king":                  "agent_seed_king_" + runID,
	}
	for roleID, agentID := range agentsByRole {
		mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, ?, 'idle', NULL, 0, ?, ?)`, agentID, roleID, now, now)
	}
	for nodeID, roleID := range rolesByNode {
		for attempt := 1; attempt <= maxAttempt; attempt++ {
			mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, ?, ?, ?, 'succeeded', ?, ?, 1, ?, ?)`, fmt.Sprintf("nr_%s_%d_%s", strings.ReplaceAll(nodeID, "_", "-"), attempt, runID), runID, nodeID, attempt, attempt, roleID, agentsByRole[roleID], now, now)
		}
	}
}

func assertLatestAttempt(t *testing.T, db *sql.DB, runID, nodeID string, expectedAttempt int, expectedStatus string) {
	t.Helper()

	var attempt int
	var status string
	if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = ? AND node_id = ? ORDER BY attempt DESC LIMIT 1`, runID, nodeID).Scan(&attempt, &status); err != nil {
		t.Fatalf("query latest attempt for node %q: %v", nodeID, err)
	}
	if attempt != expectedAttempt || status != expectedStatus {
		t.Fatalf("expected latest attempt for node %q attempt=%d status=%q, got attempt=%d status=%q", nodeID, expectedAttempt, expectedStatus, attempt, status)
	}
}

func seedPreviewBundle(t *testing.T, db *sql.DB, runID string) {
	t.Helper()

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-preview-a', 'council-member', 'idle', NULL, 0, ?, ?)`, now, now); err != nil {
		t.Fatalf("seed preview agent a: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-preview-b', 'council-member', 'idle', NULL, 0, ?, ?)`, now, now); err != nil {
		t.Fatalf("seed preview agent b: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('node-run-preview-a', ?, 'kickoff', 1,1, 'succeeded', 'council-member', 'agent-preview-a', 1, NULL, NULL)`, runID); err != nil {
		t.Fatalf("seed preview node run a: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('node-run-preview-b', ?, 'royal_closure', 1,1, 'succeeded', 'council-member', 'agent-preview-b', 1, NULL, NULL)`, runID); err != nil {
		t.Fatalf("seed preview node run b: %v", err)
	}

	workspaceRoot := t.TempDir()
	if _, err := db.Exec(`INSERT INTO workspaces (workspace_id, run_id, node_run_id, workspace_path, status, created_at, released_at) VALUES ('ws-preview-a', ?, 'node-run-preview-a', ?, 'released', ?, ?)`, runID, workspaceRoot, now, now); err != nil {
		t.Fatalf("seed preview workspace a: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO workspaces (workspace_id, run_id, node_run_id, workspace_path, status, created_at, released_at) VALUES ('ws-preview-b', ?, 'node-run-preview-b', ?, 'released', ?, ?)`, runID, workspaceRoot, now, now); err != nil {
		t.Fatalf("seed preview workspace b: %v", err)
	}

	insertArtifact := func(artifactID string, nodeRunID string, attemptID string, artifactKind string, name string, mime string, relPath string, body string, createdAt string) {
		t.Helper()
		sum := sha256.Sum256([]byte(body))
		checksum := hex.EncodeToString(sum[:])
		if _, err := db.Exec(
			`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'council-member', ?)`,
			artifactID,
			runID,
			nodeRunID,
			attemptID,
			artifactKind,
			name,
			mime,
			relPath,
			checksum,
			len(body),
			createdAt,
		); err != nil {
			t.Fatalf("seed preview artifact %s: %v", artifactID, err)
		}
		if _, err := db.Exec(`INSERT INTO artifact_contents (artifact_id, content, sha256, size_bytes, created_at) VALUES (?, ?, ?, ?, ?)`, artifactID, []byte(body), checksum, len(body), createdAt); err != nil {
			t.Fatalf("seed preview artifact content %s: %v", artifactID, err)
		}
	}

	htmlBody := `<!doctype html><html><head><link rel="stylesheet" href="./styles.css"></head><body><h1>Preview Bundle</h1><script src="./app.js"></script></body></html>`
	insertArtifact("artifact-html-root", "node-run-preview-a", "attempt-preview-a", "deliverable", "index.html", "text/html", "web/index.html", htmlBody, now)
	insertArtifact("artifact-css-primary", "node-run-preview-a", "attempt-preview-a", "deliverable", "styles.css", "text/css", "web/styles.css", "body { background: #123456; }", now)
	insertArtifact("artifact-js-primary", "node-run-preview-a", "attempt-preview-a", "deliverable", "app.js", "application/javascript", "web/app.js", "console.log('preview bundle');", now)
	insertArtifact("artifact-css-shadow", "node-run-preview-b", "attempt-preview-b", "verification", "styles.css", "text/css", "web/styles.css", "body { background: #ff0000; }", now)
}

func seedArtifactIdentityPair(t *testing.T, db *sql.DB, runID string) {
	t.Helper()

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-identity', 'council-member', 'idle', NULL, 0, ?, ?)`, now, now); err != nil {
		t.Fatalf("seed identity agent: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('node-run-identity', ?, 'kickoff', 1,1, 'pending', 'council-member', 'agent-identity', 1, NULL, NULL)`, runID); err != nil {
		t.Fatalf("seed identity node run: %v", err)
	}
	workspaceRoot := t.TempDir()
	if _, err := db.Exec(`INSERT INTO workspaces (workspace_id, run_id, node_run_id, workspace_path, status, created_at, released_at) VALUES ('ws-identity', ?, 'node-run-identity', ?, 'released', ?, ?)`, runID, workspaceRoot, now, now); err != nil {
		t.Fatalf("seed identity workspace: %v", err)
	}

	insertIdentity := func(artifactID string, body string, createdAt string) {
		t.Helper()
		sum := sha256.Sum256([]byte(body))
		checksum := hex.EncodeToString(sum[:])
		if _, err := db.Exec(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (?, ?, 'node-run-identity', 'attempt-identity', 'deliverable', 'index.html', 'text/html', 'index.html', ?, ?, 'council-member', ?)`, artifactID, runID, checksum, len(body), createdAt); err != nil {
			t.Fatalf("seed identity artifact %s: %v", artifactID, err)
		}
		if _, err := db.Exec(`INSERT INTO artifact_contents (artifact_id, content, sha256, size_bytes, created_at) VALUES (?, ?, ?, ?, ?)`, artifactID, []byte(body), checksum, len(body), createdAt); err != nil {
			t.Fatalf("seed identity artifact content %s: %v", artifactID, err)
		}
	}

	insertIdentity("artifact-identity-1", "alpha-homepage", now)
	insertIdentity("artifact-identity-2", "omega-homepage", now)
}

func setRunPlanState(t *testing.T, db *sql.DB, runID string, state string) {
	t.Helper()

	if _, err := db.Exec(`UPDATE workflow_runs SET plan_state = ?, updated_at = ? WHERE run_id = ?`, state, time.Now().UTC().Format(time.RFC3339), runID); err != nil {
		t.Fatalf("set run plan state %q: %v", state, err)
	}
}

func setRunStatus(t *testing.T, db *sql.DB, runID string, status string) {
	t.Helper()

	if _, err := db.Exec(`UPDATE workflow_runs SET run_status = ?, updated_at = ? WHERE run_id = ?`, status, time.Now().UTC().Format(time.RFC3339), runID); err != nil {
		t.Fatalf("set run status %q: %v", status, err)
	}
}

func assertErrorCode(t *testing.T, rawBody []byte, want string) {
	t.Helper()

	envelope := decodeCanonicalErrorEnvelope(t, rawBody)

	if string(envelope.Error.Code) != want {
		t.Fatalf("expected error code %q, got %q", want, envelope.Error.Code)
	}
}

func decodeCanonicalErrorEnvelope(t *testing.T, rawBody []byte) canonicalErrorEnvelope {
	t.Helper()

	var envelope canonicalErrorEnvelope
	if err := json.Unmarshal(rawBody, &envelope); err != nil {
		t.Fatalf("decode error envelope: %v; body=%s", err, string(rawBody))
	}
	return envelope
}

func assertCanonicalError(t *testing.T, rawBody []byte, wantCode string, wantCorrelationID string, wantCategory string, wantRetryable bool) {
	t.Helper()

	envelope := decodeCanonicalErrorEnvelope(t, rawBody)
	if string(envelope.Error.Code) != wantCode {
		t.Fatalf("expected error code %q, got %q", wantCode, envelope.Error.Code)
	}
	if envelope.Error.CorrelationID != wantCorrelationID {
		t.Fatalf("expected correlation ID %q, got %q", wantCorrelationID, envelope.Error.CorrelationID)
	}
	if string(envelope.Error.Category) != wantCategory {
		t.Fatalf("expected category %q, got %q", wantCategory, envelope.Error.Category)
	}
	if envelope.Error.Retryable != wantRetryable {
		t.Fatalf("expected retryable=%t, got %t", wantRetryable, envelope.Error.Retryable)
	}
	if envelope.Error.OccurredAt.IsZero() {
		t.Fatal("expected occurred_at to be populated")
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func mustExecSQL(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec failed: %v\nquery: %s", err, query)
	}
}
