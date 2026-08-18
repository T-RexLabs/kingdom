package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"kingdom/internal/api"
	gen "kingdom/internal/apigen"
	"kingdom/internal/bootstrapassets"
	"kingdom/internal/core/schemas"
	"kingdom/internal/dbmigrations"
	"kingdom/internal/external"
	"kingdom/internal/idempotency"
	kingdom "kingdom/internal/orchestration"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"

	_ "modernc.org/sqlite"
)

const e2eOperatorCredential = "e2e-operator-credential"

func TestE2ERetryAndRerunFlowsAccepted(t *testing.T) {
	t.Parallel()

	env := newE2EEnv(t)
	runID := env.createRun(t)
	env.setRunStatus(t, runID, "failed")

	retryRes := env.request(t, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/nodes/execution_kickoff/retry",
		body: map[string]any{
			"reason": "recover flaky downstream",
		},
		idempotencyKey: "idem-h3-retry",
	})
	if retryRes.Code != http.StatusAccepted {
		t.Fatalf("expected 202 for retry, got %d (%s)", retryRes.Code, retryRes.Body.String())
	}
	env.setRunStatus(t, runID, "failed")

	rerunRes := env.request(t, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/rerun-from/execution_kickoff",
		body: map[string]any{
			"reason": "rerun after policy update",
		},
		idempotencyKey: "idem-h3-rerun",
	})
	if rerunRes.Code != http.StatusAccepted {
		t.Fatalf("expected 202 for rerun, got %d (%s)", rerunRes.Code, rerunRes.Body.String())
	}
}

func TestE2ERetryRejectedWhenRunNotFailed(t *testing.T) {
	t.Parallel()

	env := newE2EEnv(t)
	runID := env.createRun(t)

	retryRes := env.request(t, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/nodes/execution_kickoff/retry",
		body: map[string]any{
			"reason": "retry while still active",
		},
		idempotencyKey: "idem-h3-retry-conflict",
	})
	if retryRes.Code != http.StatusConflict {
		t.Fatalf("expected 409 retry conflict, got %d (%s)", retryRes.Code, retryRes.Body.String())
	}
}

func TestE2EReconnectFlowUsesLastEventID(t *testing.T) {
	t.Parallel()

	env := newE2EEnv(t)
	runID := env.createRun(t)
	env.seedAttemptEvents(t, runID)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/stream?run_id="+runID, nil)
	req.RemoteAddr = "127.0.0.1:49152"
	req.Header.Set("Authorization", "Bearer "+e2eOperatorCredential)
	req.Header.Set("Last-Event-ID", "evt-h3-2")

	res := httptest.NewRecorder()
	env.handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 from stream reconnect, got %d (%s)", res.Code, res.Body.String())
	}
	body := res.Body.String()
	if strings.Contains(body, "id: evt-h3-1") || strings.Contains(body, "id: evt-h3-2") {
		t.Fatalf("expected reconnect stream to exclude previous events, got %s", body)
	}
	if !strings.Contains(body, "id: evt-h3-3") {
		t.Fatalf("expected reconnect stream to include evt-h3-3, got %s", body)
	}
}

func TestE2EWaiverEndpointRemoved(t *testing.T) {
	t.Parallel()

	env := newE2EEnv(t)
	runID := env.createRun(t)

	res := env.request(t, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/waivers",
		body: map[string]any{
			"run_id":      runID,
			"waiver_type": "emergency_override",
			"target_gate": "security_validation",
			"reason":      "legacy waiver route should be removed",
		},
		idempotencyKey: "idem-h3-waiver-removed",
	})
	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for removed waiver endpoint, got %d (%s)", res.Code, res.Body.String())
	}
}

func TestE2ESeededExternalSecretLeakContainment(t *testing.T) {
	privateKeyMarker := "GITHUB_PRIVATE_KEY_MARKER_0b91"
	installationTokenMarker := "INSTALLATION_TOKEN_MARKER_e88a"
	privateBodyMarker := "PRIVATE_ISSUE_BODY_MARKER_41ce"
	adapter := &deterministicExternalAdapter{issue: state.NormalizedExternalIssue{Title: "Deterministic issue", Body: privateBodyMarker, State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: "2026-03-01T12:00:00Z", UpdatedAt: "2026-03-01T13:00:00Z"}, token: installationTokenMarker}
	env := newE2EEnvWithExternalAdapter(t, adapter)
	secretDir := filepath.Join(env.projectRoot, ".kingdom", "secrets", "github-apps", "connection-fixture")
	if err := os.MkdirAll(secretDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(secretDir, "private-key.pem"), []byte(privateKeyMarker), 0o600); err != nil {
		t.Fatal(err)
	}

	responses := strings.Builder{}
	create := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/connections", idempotencyKey: "leak-connection", body: map[string]any{"provider": "github", "credential_strategy": "app_private_key", "display_name": "Leak fixture", "app_id": "1", "client_id": "Iv1", "app_slug": "app", "installation_id": "2"}})
	responses.WriteString(create.Body.String())
	if create.Code != http.StatusCreated {
		t.Fatalf("create connection: %d %s", create.Code, create.Body.String())
	}
	var connection struct {
		ConnectionID string `json:"connection_id"`
	}
	if err := json.Unmarshal(create.Body.Bytes(), &connection); err != nil {
		t.Fatal(err)
	}
	health := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/connections/" + connection.ConnectionID + "/health-checks", idempotencyKey: "leak-health"})
	responses.WriteString(health.Body.String())
	capture := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-captures", idempotencyKey: "leak-capture", body: map[string]any{"connection_id": connection.ConnectionID, "repository_id": "200", "issue_number": 7}})
	responses.WriteString(capture.Body.String())
	if capture.Code != http.StatusCreated {
		t.Fatalf("capture: %d %s", capture.Code, capture.Body.String())
	}

	for _, marker := range []string{e2eOperatorCredential, privateKeyMarker, installationTokenMarker, privateBodyMarker} {
		if strings.Contains(responses.String(), marker) {
			t.Fatalf("compact mutation response leaked marker %q: %s", marker, responses.String())
		}
	}
	var idempotencyPayloads, auditPayloads string
	if err := env.db.QueryRow(`SELECT COALESCE(group_concat(response_headers_json || response_body_json, ''), '') FROM idempotency_keys`).Scan(&idempotencyPayloads); err != nil {
		t.Fatal(err)
	}
	if err := env.db.QueryRow(`SELECT COALESCE(group_concat(safe_payload_json, ''), '') FROM external_integration_audit_events`).Scan(&auditPayloads); err != nil {
		t.Fatal(err)
	}
	for surface, payload := range map[string]string{"idempotency": idempotencyPayloads, "audit": auditPayloads} {
		for _, marker := range []string{e2eOperatorCredential, privateKeyMarker, installationTokenMarker, privateBodyMarker} {
			if strings.Contains(payload, marker) {
				t.Fatalf("%s persistence leaked marker %q: %s", surface, marker, payload)
			}
		}
	}

	backupPath := filepath.Join(t.TempDir(), "kingdom-backup.sqlite")
	if _, err := env.db.Exec(`VACUUM INTO ?`, backupPath); err != nil {
		t.Fatalf("vacuum backup: %v", err)
	}
	backup, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{e2eOperatorCredential, privateKeyMarker, installationTokenMarker} {
		if bytes.Contains(backup, []byte(marker)) {
			t.Fatalf("SQLite backup leaked non-database secret %q", marker)
		}
	}
	if !bytes.Contains(backup, []byte(privateBodyMarker)) {
		t.Fatal("SQLite backup omitted authoritative external snapshot content")
	}
}

func TestE2EDeterministicExternalDiscoveryIntakeIssuanceAndRefresh(t *testing.T) {
	adapter := &deterministicExternalAdapter{issue: state.NormalizedExternalIssue{Title: "Deterministic issue", Body: "Provider-neutral intake body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: "2026-03-01T12:00:00Z", UpdatedAt: "2026-03-01T13:00:00Z"}}
	env := newE2EEnvWithExternalAdapter(t, adapter)
	create := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/connections", idempotencyKey: "e2e-external-connection", body: map[string]any{"provider": "github", "credential_strategy": "app_private_key", "display_name": "Fixture", "app_id": "1", "client_id": "Iv1", "app_slug": "app", "installation_id": "2"}})
	if create.Code != http.StatusCreated {
		t.Fatalf("create connection: %d %s", create.Code, create.Body.String())
	}
	var connection struct {
		ConnectionID string `json:"connection_id"`
	}
	if err := json.Unmarshal(create.Body.Bytes(), &connection); err != nil {
		t.Fatal(err)
	}
	health := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/connections/" + connection.ConnectionID + "/health-checks", idempotencyKey: "e2e-external-health"})
	if health.Code != http.StatusOK {
		t.Fatalf("health: %d %s", health.Code, health.Body.String())
	}
	for _, path := range []string{"/api/v1/connections/" + connection.ConnectionID + "/repositories", "/api/v1/connections/" + connection.ConnectionID + "/repositories/200/issues"} {
		if response := env.request(t, requestSpec{method: http.MethodGet, path: path}); response.Code != http.StatusOK {
			t.Fatalf("discovery %s: %d %s", path, response.Code, response.Body.String())
		}
	}
	capture := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-captures", idempotencyKey: "e2e-external-capture", body: map[string]any{"connection_id": connection.ConnectionID, "repository_id": "200", "issue_number": 7}})
	if capture.Code != http.StatusCreated {
		t.Fatalf("capture: %d %s", capture.Code, capture.Body.String())
	}
	var captured struct {
		SnapshotID string `json:"snapshot_id"`
	}
	if err := json.Unmarshal(capture.Body.Bytes(), &captured); err != nil {
		t.Fatal(err)
	}
	decree := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/decrees", idempotencyKey: "e2e-external-decree", body: map[string]any{"decree_title": "Operator edit", "decree_text": "Immutable operator text", "external_source_snapshot_id": captured.SnapshotID}})
	if decree.Code != http.StatusCreated {
		t.Fatalf("decree: %d %s", decree.Code, decree.Body.String())
	}
	planning := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/planning-sessions", idempotencyKey: "e2e-external-planning", body: map[string]any{"title": "External intake"}})
	if planning.Code != http.StatusCreated {
		t.Fatalf("planning session: %d %s", planning.Code, planning.Body.String())
	}
	var planningSession struct {
		Session struct {
			PlanningSessionID string `json:"planning_session_id"`
		} `json:"session"`
	}
	if err := json.Unmarshal(planning.Body.Bytes(), &planningSession); err != nil {
		t.Fatal(err)
	}
	attach := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/planning-sessions/" + planningSession.Session.PlanningSessionID + "/external-sources", idempotencyKey: "e2e-external-attach", body: map[string]any{"connection_id": connection.ConnectionID, "repository_id": "200", "issue_number": 7}})
	if attach.Code != http.StatusCreated {
		t.Fatalf("planning intake: %d %s", attach.Code, attach.Body.String())
	}
	if count := env.queryInt(t, `SELECT COUNT(*) FROM external_sources`); count != 1 {
		t.Fatalf("canonical source count=%d", count)
	}
	if count := env.queryInt(t, `SELECT COUNT(*) FROM external_source_links`); count != 2 {
		t.Fatalf("link count=%d", count)
	}
	var linkID string
	if err := env.db.QueryRow(`SELECT link_id FROM external_source_links WHERE target_kind='decree'`).Scan(&linkID); err != nil {
		t.Fatal(err)
	}
	adapter.update("Changed deterministic issue", "Provider-neutral intake body", "2026-03-01T14:00:00Z")
	refresh := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + linkID + "/refreshes", idempotencyKey: "e2e-external-refresh"})
	if refresh.Code != http.StatusOK || !strings.Contains(refresh.Body.String(), `"outcome":"changed"`) {
		t.Fatalf("refresh: %d %s", refresh.Code, refresh.Body.String())
	}
	var decreeText string
	if err := env.db.QueryRow(`SELECT decree_text FROM decrees WHERE decree_title='Operator edit'`).Scan(&decreeText); err != nil || decreeText != "Immutable operator text" {
		t.Fatalf("decree text=%q err=%v", decreeText, err)
	}
}

type e2eEnv struct {
	handler     http.Handler
	db          *sql.DB
	ids         *testIDGenerator
	runtime     *kingdom.Runtime
	projectRoot string
}

type testIDGenerator struct {
	next atomic.Uint64
}

func (g *testIDGenerator) Next(prefix string) string {
	return fmt.Sprintf("%s-h3-%04d", prefix, g.next.Add(1))
}

type requestSpec struct {
	method         string
	path           string
	body           map[string]any
	idempotencyKey string
}

type staticProviderModelCatalog struct {
	items []gen.ProviderModel
}

func (s staticProviderModelCatalog) List(_ context.Context, _ bool) ([]gen.ProviderModel, error) {
	rows := make([]gen.ProviderModel, len(s.items))
	copy(rows, s.items)
	return rows, nil
}

func (s staticProviderModelCatalog) Snapshot(_ context.Context, _ bool) (api.ProviderModelCatalogSnapshot, error) {
	rows := make([]gen.ProviderModel, len(s.items))
	copy(rows, s.items)
	return api.ProviderModelCatalogSnapshot{Items: rows, Status: api.ProviderModelCatalogStatusFresh}, nil
}

func newE2EEnv(t *testing.T) *e2eEnv {
	return newE2EEnvWithExternalAdapter(t, nil)
}

func newE2EEnvWithExternalAdapter(t *testing.T, externalAdapter external.Adapter) *e2eEnv {
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

	migrationNames, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	for _, name := range migrationNames {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		if _, err := db.Exec(string(raw)); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}

	seedRoles(t, db)
	projectRoot := t.TempDir()
	if err := bootstrapassets.WriteScaffold(filepath.Join(projectRoot, ".kingdom"), projectRoot, false); err != nil {
		t.Fatalf("write managed opencode scaffold: %v", err)
	}

	generator := &testIDGenerator{}
	handler, err := api.NewHandler(api.ServerConfig{
		DB:                 db,
		IDGenerator:        generator.Next,
		ProjectRoot:        projectRoot,
		ProviderEngine:     "opencode",
		OpenCodeBinaryPath: "/bin/sh",
		ProviderModelCatalog: staticProviderModelCatalog{items: []gen.ProviderModel{{
			Id:                    "github-copilot/gpt-5.3-codex",
			ProviderId:            "github-copilot",
			ProviderName:          "GitHub Copilot",
			ModelName:             "GPT-5.3-Codex",
			ThinkingEffortOptions: []gen.ThinkingEffortOption{{Value: "low", Label: "Low"}},
		}}},
		Now: func() time.Time {
			return time.Date(2026, 3, 1, 13, 0, 0, 0, time.UTC)
		},
		OperatorCredential: []byte(e2eOperatorCredential),
		ExternalAdapter:    externalAdapter,
	})
	if err != nil {
		t.Fatalf("build api handler: %v", err)
	}

	runtimeInstance, err := kingdom.NewRuntime(kingdom.Config{
		DB:               db,
		Paths:            kingdom.DefaultBootstrapPaths(repoRoot(t)),
		Adapter:          opencode.NewMemoryAdapter(),
		ProviderEngine:   "opencode",
		DispatchInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("build runtime: %v", err)
	}

	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())
	if err := runtimeInstance.Start(runtimeCtx); err != nil {
		runtimeCancel()
		t.Fatalf("start runtime: %v", err)
	}
	t.Cleanup(func() {
		runtimeCancel()
		if err := runtimeInstance.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	})

	return &e2eEnv{handler: handler, db: db, ids: generator, runtime: runtimeInstance, projectRoot: projectRoot}
}

func (e *e2eEnv) request(t *testing.T, spec requestSpec) *httptest.ResponseRecorder {
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
	req.RemoteAddr = "127.0.0.1:49152"
	req.Header.Set("Authorization", "Bearer "+e2eOperatorCredential)
	if spec.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if spec.idempotencyKey != "" {
		digest := sha256.Sum256([]byte(spec.idempotencyKey))
		key, err := idempotency.GenerateWithReader("test", time.Now().UTC().Truncate(time.Hour), idempotency.DefaultTTL, bytes.NewReader(digest[:]))
		if err != nil {
			t.Fatalf("generate idempotency key: %v", err)
		}
		req.Header.Set("Idempotency-Key", key)
	}

	res := httptest.NewRecorder()
	e.handler.ServeHTTP(res, req)
	return res
}

func (e *e2eEnv) createRun(t *testing.T) string {
	t.Helper()

	decreeRes := e.request(t, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/decrees",
		body: map[string]any{
			"decree_title": "H3 decree",
			"decree_text":  "H3 decree text",
		},
		idempotencyKey: "idem-h3-decree",
	})
	if decreeRes.Code != http.StatusCreated {
		t.Fatalf("create decree expected 201, got %d (%s)", decreeRes.Code, decreeRes.Body.String())
	}

	var decree struct {
		DecreeID string `json:"decree_id"`
	}
	if err := json.Unmarshal(decreeRes.Body.Bytes(), &decree); err != nil {
		t.Fatalf("decode decree response: %v", err)
	}

	publishRes := e.request(t, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": "royal-decree-v1",
			"version":     "h3-e2e",
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": "royal-decree-v1",
				"nodes": []map[string]any{
					{"node_id": "execution_kickoff", "required_role": "council-member", "node_class": "execution_kickoff"},
					{"node_id": "delegated_execution", "required_role": "builder-berserker", "node_class": "delegated_execution"},
				},
				"edges": []map[string]any{
					{"from": "execution_kickoff", "to": "delegated_execution", "on": "success"},
				},
			},
		},
		idempotencyKey: "idem-h3-publish",
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("publish workflow expected 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
	}

	runRes := e.request(t, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"decree_id":    decree.DecreeID,
			"publish_mode": "direct",
			"model_selection": map[string]any{
				"global": map[string]any{"model": "github-copilot/gpt-5.3-codex"},
			},
		},
		idempotencyKey: "idem-h3-run",
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("create run expected 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}

	var run struct {
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run response: %v", err)
	}
	return run.RunID
}

func (e *e2eEnv) queryInt(t *testing.T, query string, args ...any) int {
	t.Helper()

	var value int
	if err := e.db.QueryRow(query, args...).Scan(&value); err != nil {
		t.Fatalf("query int failed: %v", err)
	}
	return value
}

func (e *e2eEnv) seedAttemptEvents(t *testing.T, runID string) {
	t.Helper()

	ingestor := state.NewAttemptEventIngestor(e.db, loadEventSchemas(t))
	now := "2026-03-01T13:00:00Z"

	for i := 1; i <= 3; i++ {
		payload, err := json.Marshal(map[string]any{"step": i})
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		if _, err := ingestor.Ingest(context.Background(), state.AttemptEventIngestionRequest{
			EventID:         "evt-h3-" + string(rune('0'+i)),
			RunID:           runID,
			NodeRunID:       sql.NullString{},
			AttemptID:       "attempt-h3-stream",
			SchemaVersion:   "v1",
			EventType:       "attempt.progress",
			Seq:             i,
			PayloadJSON:     string(payload),
			ActorID:         "actor-chancellor",
			RoleID:          "chancellor",
			AuthoritySource: "test.e2e",
			ReasonCode:      "internal_error",
			ReasonText:      "seed",
			CorrelationID:   "corr-h3",
			CausationID:     "caus-h3",
			CreatedAt:       now,
		}); err != nil {
			t.Fatalf("seed event %d: %v", i, err)
		}
	}
}

func (e *e2eEnv) setRunStatus(t *testing.T, runID, status string) {
	t.Helper()
	if _, err := e.db.Exec(`UPDATE workflow_runs SET run_status = ?, updated_at = ? WHERE run_id = ?`, status, time.Now().UTC().Format(time.RFC3339), runID); err != nil {
		t.Fatalf("set run status %q failed: %v", status, err)
	}
}

func seedRoles(t *testing.T, db *sql.DB) {
	t.Helper()

	roles := []string{"king", "chancellor", "council-member", "builder", "intelligence"}
	now := "2026-03-01T13:00:00Z"

	for _, roleID := range roles {
		if _, err := db.Exec(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES (?, ?, 1, ?)`, roleID, roleID, now); err != nil {
			t.Fatalf("seed role %s: %v", roleID, err)
		}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()

	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	return root
}

func loadEventSchemas(t *testing.T) *schemas.Set {
	t.Helper()

	set, err := schemas.Load(filepath.Join(repoRoot(t), "contracts", "schemas"))
	if err != nil {
		t.Fatalf("load event schemas: %v", err)
	}
	return set
}
