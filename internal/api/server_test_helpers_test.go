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
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/workflow"
	"kingdom/internal/dbmigrations"
	"kingdom/internal/idempotency"
	"kingdom/internal/planning"
	"kingdom/internal/state"
	"kingdom/internal/testutil/planningfixture"
	"kingdom/internal/testutil/providerledger"
)

type testPlannerAgent struct{}

// apiTestTerminalMetadata models a planner turn that settles server-side: the
// provider reports a terminal success outcome and the runtime assembles the
// final text. The turn plays the shared server for its runner-issued
// invocation, which binds the request and records its terminal evidence.
func apiTestTerminalMetadata(input planning.AgentContext, finalText string) (planning.ProviderExecutionMetadata, error) {
	invocation := input.ProviderInvocation
	if invocation == nil || invocation.Activate == nil {
		return planning.ProviderExecutionMetadata{}, errors.New("a fake planner turn requires its runner-issued invocation")
	}
	response, err := providerledger.Complete(context.Background(), apiTestLedgerInvocation(invocation), finalText)
	if err != nil {
		return planning.ProviderExecutionMetadata{}, err
	}
	return planning.ProviderExecutionMetadata{
		FinalText:                       finalText,
		ProviderSessionID:               response.ProviderSessionID,
		OutcomeState:                    "success",
		CompletionStructuredPayloadJSON: "{}",
		StreamObservation:               response.StreamObservation,
	}, nil
}

func apiTestLedgerInvocation(invocation *planning.ProviderInvocationContext) providerledger.Invocation {
	return providerledger.Invocation{ID: invocation.InvocationID, ReusableSessionID: invocation.ReusableProviderSessionID, Activate: invocation.Activate}
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
	metadata, err := apiTestTerminalMetadata(input, defaultPlannerAPITestScript.ContinueContent)
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
	metadata, err := apiTestTerminalMetadata(input, defaultPlannerAPITestScript.FormalizedText)
	if err != nil {
		return planning.FormalizedPlan{}, err
	}
	return planning.FormalizedPlan{
		WorkOrderTitle: input.Title,
		WorkOrderText:  defaultPlannerAPITestScript.FormalizedText,
		Metadata:       metadata,
	}, nil
}

func (a *testPlannerAgent) GenerateTitle(_ context.Context, input planning.AgentContext, onChunk func(text string)) (planning.GeneratedTitle, error) {
	metadata, err := apiTestTerminalMetadata(input, defaultPlannerAPITestScript.GeneratedTitle)
	if err != nil {
		return planning.GeneratedTitle{}, err
	}
	if onChunk != nil {
		onChunk(defaultPlannerAPITestScript.GeneratedTitle)
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
	metadata, err := apiTestTerminalMetadata(input, a.final)
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

func (a *testStreamingPlannerAgent) GenerateTitle(_ context.Context, input planning.AgentContext, onChunk func(text string)) (planning.GeneratedTitle, error) {
	return (&testPlannerAgent{}).GenerateTitle(context.Background(), input, onChunk)
}

type requestSpec struct {
	method         string
	path           string
	body           map[string]any
	headers        map[string]string
	idempotencyKey string
}

const apiTestOperatorCredential = "api-test-operator-credential"

func authenticateAPITestRequest(req *http.Request) {
	req.RemoteAddr = "127.0.0.1:49152"
	req.Header.Set("Authorization", "Bearer "+apiTestOperatorCredential)
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
	authenticateAPITestRequest(req)
	for name, value := range spec.headers {
		req.Header.Set(name, value)
	}
	if spec.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if spec.idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", testIdempotencyKey(t, spec.idempotencyKey, time.Now()))
	}

	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	return res
}

func testIdempotencyKey(t *testing.T, logicalKey string, now time.Time) string {
	t.Helper()
	if strings.HasPrefix(logicalKey, idempotency.Version+".") {
		return logicalKey
	}
	digest := sha256.Sum256([]byte(logicalKey))
	key, err := idempotency.GenerateWithReader("test", now.UTC().Truncate(time.Hour), idempotency.DefaultTTL, bytes.NewReader(digest[:]))
	if err != nil {
		t.Fatalf("generate test idempotency key: %v", err)
	}
	return key
}

func newAPIServerForTest(t *testing.T) (http.Handler, *sql.DB) {
	t.Helper()
	return newAPIServerForTestWithConfig(t, ServerConfig{})
}

func seedBrowserSessionForTest(t *testing.T, server *Server, rawOrigin string, expiresAt time.Time) (*http.Cookie, browserSession) {
	t.Helper()
	parsed, err := url.Parse(rawOrigin)
	if err != nil {
		t.Fatalf("parse browser session origin: %v", err)
	}
	origin, local, err := canonicalOrigin(parsed.Scheme, parsed.Host)
	if err != nil || !local {
		t.Fatalf("canonicalize browser session origin %q: local=%t err=%v", rawOrigin, local, err)
	}
	lease, _, err := server.auth.createOrReuseSession("", origin)
	if err != nil {
		t.Fatalf("create browser session: %v", err)
	}
	key := sha256.Sum256([]byte(lease.CookieToken))
	server.auth.mu.Lock()
	session := server.auth.sessions[key]
	session.ExpiresAt = expiresAt
	server.auth.sessions[key] = session
	server.auth.mu.Unlock()
	return &http.Cookie{Name: browserSessionCookieName(origin, server.basePath), Value: lease.CookieToken, Path: server.basePath + "/"}, session
}

func newAPIServerForTestWithConfig(t *testing.T, cfg ServerConfig) (http.Handler, *sql.DB) {
	t.Helper()

	db := cfg.DB
	if db == nil {
		db = openAPIServerTestDB(t)
	}

	cfg.DB = db
	if len(cfg.OperatorCredential) == 0 {
		cfg.OperatorCredential = []byte(apiTestOperatorCredential)
	}
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
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := handler.Close(closeCtx); err != nil {
			t.Errorf("close api test server: %v", err)
		}
	})

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
	t.Cleanup(providerledger.Register(db))

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
	root := t.TempDir()
	commands := [][]string{
		{"git", "init", "-q", root},
		{"git", "-C", root, "config", "user.name", "Kingdom Tests"},
		{"git", "-C", root, "config", "user.email", "kingdom-tests@invalid.local"},
	}
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("test repository\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	commands = append(commands,
		[]string{"git", "-C", root, "add", "README.md"},
		[]string{"git", "-C", root, "commit", "-q", "-m", "test fixture"},
	)
	for _, args := range commands {
		if output, err := exec.Command(args[0], args[1:]...).CombinedOutput(); err != nil {
			t.Fatalf("prepare git test repository: %v (%s)", err, output)
		}
	}
	return root
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
		"governance",
		"operator",
		"coordinator",
		"plan-author",
		"architecture-advisor",
		"risk-advisor",
		"systems-advisor",
		"usability-advisor",
		"builder",
		"reviewer",
		"qa-validator",
		"security-validator",
		"resilience-validator",
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

	workOrderRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/work-orders",
		body: map[string]any{
			"work_order_title": "Seed work order",
			"work_order_text":  "Seed text",
		},
		idempotencyKey: "idem-seed-work order",
	})
	if workOrderRes.Code != http.StatusCreated {
		t.Fatalf("seed work order expected 201, got %d (%s)", workOrderRes.Code, workOrderRes.Body.String())
	}

	var workOrder gen.CreateWorkOrderResult
	if err := json.Unmarshal(workOrderRes.Body.Bytes(), &workOrder); err != nil {
		t.Fatalf("decode work order response: %v", err)
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
				"nodes":       []map[string]any{{"node_id": "kickoff", "required_role": "plan-author", "node_class": "execution_kickoff"}},
				"edges":       []map[string]any{},
			},
		},
		idempotencyKey: "idem-seed-workflow",
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("seed workflow expected 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
	}

	runBody := map[string]any{
		"work_order_id":   workOrder.WorkOrderId,
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

func seedWorkOrder(t *testing.T, handler http.Handler) string {
	t.Helper()
	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/work-orders",
		body: map[string]any{
			"work_order_title": "Seed work order",
			"work_order_text":  "Seed text",
		},
		idempotencyKey: "idem-seed-work order",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("seed work order expected 201, got %d (%s)", res.Code, res.Body.String())
	}
	var workOrder gen.CreateWorkOrderResult
	if err := json.Unmarshal(res.Body.Bytes(), &workOrder); err != nil {
		t.Fatalf("decode work order seed: %v", err)
	}
	return workOrder.WorkOrderId
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
					"required_role": "plan-author",
					"node_class":    "execution_kickoff",
				}},
				"edges": []map[string]any{},
			},
		},
		idempotencyKey: "idem-seed-workflow-" + workflowID,
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("seed workflow expected 201, got %d (%s)", res.Code, res.Body.String())
	}
}

func seedRunWithLinearExecutionWorkflow(t *testing.T, handler http.Handler) string {
	t.Helper()

	workOrderRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/work-orders",
		body: map[string]any{
			"work_order_title": "Linear replay work order",
			"work_order_text":  "Linear replay text",
		},
		idempotencyKey: "idem-linear-work order",
	})
	if workOrderRes.Code != http.StatusCreated {
		t.Fatalf("linear work order expected 201, got %d (%s)", workOrderRes.Code, workOrderRes.Body.String())
	}

	var workOrder gen.CreateWorkOrderResult
	if err := json.Unmarshal(workOrderRes.Body.Bytes(), &workOrder); err != nil {
		t.Fatalf("decode linear work order response: %v", err)
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
					{"node_id": "plan_drafting", "required_role": "plan-author", "node_class": "plan_drafting"},
					{"node_id": "execution_kickoff", "required_role": "plan-author", "node_class": "execution_kickoff"},
					{"node_id": "implementation", "required_role": "builder", "node_class": "delegated_execution"},
					{"node_id": "execution_review", "required_role": "reviewer", "node_class": "execution_review"},
					{"node_id": "quality_validation", "required_role": "qa-validator", "node_class": "validation_gates"},
					{"node_id": "completion", "required_role": "governance", "node_class": "completion"},
				},
				"edges": []map[string]any{
					{"from": "plan_drafting", "to": "execution_kickoff", "on": "success"},
					{"from": "execution_kickoff", "to": "implementation", "on": "success"},
					{"from": "implementation", "to": "execution_review", "on": "success"},
					{"from": "execution_review", "to": "quality_validation", "on": "success"},
					{"from": "quality_validation", "to": "completion", "on": "success"},
				},
			},
		},
		idempotencyKey: "idem-linear-workflow",
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("linear workflow expected 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
	}

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"work_order_id":   workOrder.WorkOrderId,
			"publish_mode":    "direct",
			"model_selection": defaultTestRunModelSelectionBody(),
		},
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

	workOrderRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/work-orders",
		body: map[string]any{
			"work_order_title": "Custom plan replay work order",
			"work_order_text":  "Custom plan replay text",
		},
		idempotencyKey: "idem-custom-plan-work order-" + trimmedPlanNodeID,
	})
	if workOrderRes.Code != http.StatusCreated {
		t.Fatalf("custom plan work order expected 201, got %d (%s)", workOrderRes.Code, workOrderRes.Body.String())
	}

	var workOrder gen.CreateWorkOrderResult
	if err := json.Unmarshal(workOrderRes.Body.Bytes(), &workOrder); err != nil {
		t.Fatalf("decode custom plan work order response: %v", err)
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
					{"node_id": trimmedPlanNodeID, "required_role": "plan-author", "node_class": "plan_drafting"},
					{"node_id": "execution_kickoff", "required_role": "plan-author", "node_class": "execution_kickoff"},
					{"node_id": "implementation", "required_role": "builder", "node_class": "delegated_execution"},
					{"node_id": "completion", "required_role": "governance", "node_class": "completion"},
				},
				"edges": []map[string]any{
					{"from": trimmedPlanNodeID, "to": "execution_kickoff", "on": "success"},
					{"from": "execution_kickoff", "to": "implementation", "on": "success"},
					{"from": "implementation", "to": "completion", "on": "success"},
				},
			},
		},
		idempotencyKey: "idem-custom-plan-workflow-" + trimmedPlanNodeID,
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("custom plan workflow expected 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
	}

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"work_order_id":   workOrder.WorkOrderId,
			"publish_mode":    "direct",
			"model_selection": defaultTestRunModelSelectionBody(),
		},
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

	workOrderRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/work-orders",
		body: map[string]any{
			"work_order_title": "Multi plan work order",
			"work_order_text":  "Multi plan work order text",
		},
		idempotencyKey: "idem-multi-plan-work order",
	})
	if workOrderRes.Code != http.StatusCreated {
		t.Fatalf("multi plan work order expected 201, got %d (%s)", workOrderRes.Code, workOrderRes.Body.String())
	}

	var workOrder gen.CreateWorkOrderResult
	if err := json.Unmarshal(workOrderRes.Body.Bytes(), &workOrder); err != nil {
		t.Fatalf("decode multi plan work order response: %v", err)
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
					{"node_id": "advisory_synthesis", "required_role": "coordinator", "node_class": "advisory_synthesis"},
					{"node_id": "plan_drafting_scope", "required_role": "plan-author", "node_class": "plan_drafting"},
					{"node_id": "plan_drafting_delivery", "required_role": "plan-author", "node_class": "plan_drafting"},
					{"node_id": "plan_decision", "required_role": "governance", "node_class": "plan_decision"},
					{"node_id": "execution_kickoff", "required_role": "plan-author", "node_class": "execution_kickoff"},
				},
				"edges": []map[string]any{
					{"from": "advisory_synthesis", "to": "plan_drafting_scope", "on": "success"},
					{"from": "advisory_synthesis", "to": "plan_drafting_delivery", "on": "success"},
					{"from": "plan_drafting_scope", "to": "plan_decision", "on": "success"},
					{"from": "plan_drafting_delivery", "to": "plan_decision", "on": "success"},
					{"from": "plan_decision", "to": "execution_kickoff", "on": "approved"},
				},
			},
		},
		idempotencyKey: "idem-multi-plan-workflow",
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("multi plan workflow expected 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
	}

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"work_order_id":   workOrder.WorkOrderId,
			"publish_mode":    "direct",
			"model_selection": defaultTestRunModelSelectionBody(),
		},
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

	workOrderRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/work-orders",
		body: map[string]any{
			"work_order_title": "Governance branch work order",
			"work_order_text":  "Governance branch text",
		},
		idempotencyKey: "idem-governance-branch-work order",
	})
	if workOrderRes.Code != http.StatusCreated {
		t.Fatalf("governance branch work order expected 201, got %d (%s)", workOrderRes.Code, workOrderRes.Body.String())
	}

	var workOrder gen.CreateWorkOrderResult
	if err := json.Unmarshal(workOrderRes.Body.Bytes(), &workOrder); err != nil {
		t.Fatalf("decode governance branch work order response: %v", err)
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
					{"node_id": "plan_drafting", "required_role": "plan-author", "node_class": "plan_drafting"},
					{"node_id": "plan_decision", "required_role": "governance", "node_class": "plan_decision"},
					{"node_id": "execution_kickoff", "required_role": "plan-author", "node_class": "execution_kickoff"},
					{"node_id": "execution_review", "required_role": "reviewer", "node_class": "execution_review"},
					{"node_id": "implementation", "required_role": "builder", "node_class": "delegated_execution"},
					{"node_id": "completion", "required_role": "governance", "node_class": "completion"},
				},
				"edges": []map[string]any{
					{"from": "plan_drafting", "to": "plan_decision", "on": "success"},
					{"from": "plan_decision", "to": "execution_kickoff", "on": "approved"},
					{"from": "plan_decision", "to": "execution_review", "on": "needs_rework"},
					{"from": "execution_kickoff", "to": "implementation", "on": "success"},
					{"from": "implementation", "to": "completion", "on": "success"},
				},
			},
		},
		idempotencyKey: "idem-governance-branch-workflow",
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("governance branch workflow expected 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
	}

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"work_order_id":   workOrder.WorkOrderId,
			"publish_mode":    "direct",
			"model_selection": defaultTestRunModelSelectionBody(),
		},
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

	workOrderRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/work-orders",
		body: map[string]any{
			"work_order_title": "Planning wave work order",
			"work_order_text":  "Planning wave text",
		},
		idempotencyKey: "idem-planning-wave-work order",
	})
	if workOrderRes.Code != http.StatusCreated {
		t.Fatalf("planning wave work order expected 201, got %d (%s)", workOrderRes.Code, workOrderRes.Body.String())
	}

	var workOrder gen.CreateWorkOrderResult
	if err := json.Unmarshal(workOrderRes.Body.Bytes(), &workOrder); err != nil {
		t.Fatalf("decode planning wave work order response: %v", err)
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
					{"node_id": "advisory_synthesis", "required_role": "coordinator", "node_class": "advisory_synthesis"},
					{"node_id": "plan_drafting", "required_role": "plan-author", "node_class": "plan_drafting"},
					{"node_id": "plan_decision", "required_role": "governance", "node_class": "plan_decision"},
					{"node_id": "execution_kickoff", "required_role": "plan-author", "node_class": "execution_kickoff"},
				},
				"edges": []map[string]any{
					{"from": "advisory_synthesis", "to": "plan_drafting", "on": "success"},
					{"from": "plan_drafting", "to": "plan_decision", "on": "success"},
					{"from": "plan_decision", "to": "execution_kickoff", "on": "approved"},
				},
			},
		},
		idempotencyKey: "idem-planning-wave-workflow",
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("planning wave workflow expected 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
	}

	runBody := map[string]any{
		"work_order_id":   workOrder.WorkOrderId,
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
			RoleID:          "coordinator",
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
	if _, err := db.Exec(`INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-1', 'plan-author', 'idle', NULL, 0, ?, ?)`, now, now); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('node-run-1', ?, 'kickoff', 1,1, 'pending', 'plan-author', 'agent-1', 1, NULL, NULL)`, runID); err != nil {
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
	if _, err := db.Exec(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact-1', ?, 'node-run-1', 'attempt-seed', 'deliverable', 'report.txt', 'text/plain', 'artifacts/report.txt', ?, 13, 'plan-author', ?)`, runID, checksum, now); err != nil {
		t.Fatalf("seed artifact: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO artifact_content_chunks VALUES ('artifact-1', 0, ?)`, body); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO artifact_contents (artifact_id, sha256, size_bytes, created_at) VALUES ('artifact-1', ?, 13, ?)`, checksum, now); err != nil {
		t.Fatalf("seed artifact content: %v", err)
	}
}

func seedReadyPlanReworkNodes(t *testing.T, db *sql.DB, runID string) string {
	t.Helper()

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`UPDATE workflow_runs SET plan_state = 'awaiting_plan_decision' WHERE run_id = ?`, runID); err != nil {
		t.Fatalf("set run plan_state awaiting_plan_decision: %v", err)
	}

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'coordinator', 'idle', NULL, 0, ?, ?)`, "agent_coordinator_"+runID, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'plan-author', 'idle', NULL, 0, ?, ?)`, "agent_council_"+runID, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'governance', 'idle', NULL, 0, ?, ?)`, "agent_governance_"+runID, now, now)

	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'advisory_synthesis', 1,1, 'succeeded', 'coordinator', ?, 1, ?, ?)`, "nr_plan_rework_advisory_"+runID, runID, "agent_coordinator_"+runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'plan_drafting', 1,1, 'succeeded', 'plan-author', ?, 1, ?, ?)`, "nr_plan_rework_plan_"+runID, runID, "agent_council_"+runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'plan_decision', 1,1, 'pending', 'governance', ?, 1, NULL, NULL)`, "nr_plan_rework_governance_"+runID, runID, "agent_governance_"+runID)

	messageID := "msg_plan_current_" + runID
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES (?, ?, ?, ?, ?, 'status_update', 'normal', ?, ?)`,
		messageID,
		runID,
		"agent_council_"+runID,
		"agent_governance_"+runID,
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
	governanceAgentID := "agent_governance_" + trimmedNodeID + "_" + runID
	messageID := "msg_plan_" + trimmedNodeID + "_" + runID + "_" + suffix
	nodeRunID := "nr_plan_" + trimmedNodeID + "_" + runID
	attemptID := runID + ":" + trimmedNodeID + ":" + suffix
	planningOutputID := "planout_" + trimmedNodeID + "_" + runID + "_" + suffix
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'plan-author', 'idle', NULL, 0, ?, ?)`, councilAgentID, nowRFC3339, nowRFC3339)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'governance', 'idle', NULL, 0, ?, ?)`, governanceAgentID, nowRFC3339, nowRFC3339)
	mustExecSQL(t, db, `INSERT OR IGNORE INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, ?, 1,1, 'succeeded', 'plan-author', ?, 1, ?, ?)`, nodeRunID, runID, trimmedNodeID, councilAgentID, nowRFC3339, nowRFC3339)
	seedAttemptExecution(t, db, attemptID, runID, nodeRunID, nowRFC3339)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES (?, ?, ?, ?, ?, 'status_update', 'normal', ?, ?)`,
		messageID,
		runID,
		councilAgentID,
		governanceAgentID,
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
	mustExecSQL(t, db, `INSERT OR IGNORE INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES (?, ?, ?, 'committed_success', 'provider_backed', 'server_response', '', NULL, 0, '', ?, ?, ?, '', '', ?, ?)`,
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
		mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'advisory_synthesis', ?, ?, 'succeeded', 'coordinator', ?, 1, ?, ?)`, fmt.Sprintf("nr_plan_rework_advisory_%d_%s", attempt, runID), runID, attempt, attempt, "agent_coordinator_"+runID, now, now)
		mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'plan_drafting', ?, ?, 'succeeded', 'plan-author', ?, 1, ?, ?)`, fmt.Sprintf("nr_plan_rework_plan_%d_%s", attempt, runID), runID, attempt, attempt, "agent_council_"+runID, now, now)
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
		"plan_drafting":      "plan-author",
		"execution_kickoff":  "plan-author",
		"implementation":     "builder",
		"execution_review":   "reviewer",
		"quality_validation": "qa-validator",
		"completion":         "governance",
	}
	agentsByRole := map[string]string{
		"plan-author":  "agent_seed_council_" + runID,
		"builder":      "agent_seed_builder_" + runID,
		"reviewer":     "agent_seed_reviewer_" + runID,
		"qa-validator": "agent_seed_scout_" + runID,
		"governance":   "agent_seed_governance_" + runID,
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

func seedArtifactIdentityPair(t *testing.T, db *sql.DB, runID string) {
	t.Helper()

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-identity', 'plan-author', 'idle', NULL, 0, ?, ?)`, now, now); err != nil {
		t.Fatalf("seed identity agent: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('node-run-identity', ?, 'kickoff', 1,1, 'pending', 'plan-author', 'agent-identity', 1, NULL, NULL)`, runID); err != nil {
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
		if _, err := db.Exec(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, ?, ?, 'node-run-identity', 'attempt-identity', 'deliverable', 'index.html', 'text/html', 'index.html', ?, ?, 'plan-author', ?)`, artifactID, runID, checksum, len(body), createdAt); err != nil {
			t.Fatalf("seed identity artifact %s: %v", artifactID, err)
		}
		if _, err := db.Exec(`INSERT INTO artifact_content_chunks VALUES (?, 0, ?)`, artifactID, []byte(body)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO artifact_contents (artifact_id, sha256, size_bytes, created_at) VALUES (?, ?, ?, ?)`, artifactID, checksum, len(body), createdAt); err != nil {
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

func bindAPIPlanningContext(t *testing.T, store *state.PlanningStore, sessionID string) {
	t.Helper()
	snapshotID := "context_" + sessionID
	if _, err := store.CreateContextSnapshot(t.Context(), state.CreatePlanningContextSnapshotRequest{
		PlanningContextSnapshotID: snapshotID, PlanningSessionID: sessionID, SummaryMarkdown: "test context",
		IncludedFilesJSON: `[]`, ExcludedFilesJSON: `[]`, CreatedAt: "2026-06-12T09:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PublishContextSnapshot(t.Context(), sessionID, snapshotID, "2026-06-12T09:00:00Z"); err != nil {
		t.Fatal(err)
	}
}
