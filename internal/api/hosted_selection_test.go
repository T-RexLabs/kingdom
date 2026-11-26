package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	gen "kingdom/internal/apigen"
	"kingdom/internal/execution/access"
	"kingdom/internal/planning"
	"kingdom/internal/state"
)

type selectionResolverFunc func(context.Context, string, string, access.RequestedSelection) (access.Selection, error)

func (f selectionResolverFunc) Resolve(ctx context.Context, actor, project string, r access.RequestedSelection) (access.Selection, error) {
	return f(ctx, actor, project, r)
}
func TestHostedSelectionUsesTrustedActorAndExactTuple(t *testing.T) {
	actor, project, connection := uuid.NewString(), uuid.NewString(), uuid.NewString()
	requested := access.RequestedSelection{Model: "provider/model", ThinkingEffort: "medium", Connection: &access.ConnectionReference{ID: connection, Revision: 2}}
	raw, _ := json.Marshal(requested)
	cfg := &HostedSelectionConfig{ProjectID: project, Resolver: selectionResolverFunc(func(_ context.Context, a, p string, r access.RequestedSelection) (access.Selection, error) {
		if a != actor || p != project || *r.Connection != *requested.Connection {
			t.Fatal("untrusted actor or selection")
		}
		return access.Selection{Model: r.Model, ThinkingEffort: r.ThinkingEffort, Connection: access.Connection{ID: r.Connection.ID, Revision: r.Connection.Revision, OwnerID: a, Scope: "personal", Provider: "provider"}}, nil
	})}
	if _, err := resolveHostedSelection(context.Background(), cfg, string(raw)); err == nil {
		t.Fatal("missing principal accepted")
	}
	ctx := context.WithValue(context.Background(), principalContextKey{}, Principal{ActorID: actor})
	snapshot, err := resolveHostedSelection(ctx, cfg, string(raw))
	if err != nil || snapshot.ActorID != actor || snapshot.Selection.Connection.Revision != 2 {
		t.Fatal(snapshot, err)
	}
	original := cfg.Resolver
	for _, change := range []string{"model", "effort", "revision", "connection", "owner"} {
		t.Run(change, func(t *testing.T) {
			cfg.Resolver = selectionResolverFunc(func(ctx context.Context, a, p string, r access.RequestedSelection) (access.Selection, error) {
				s, err := original.Resolve(ctx, a, p, r)
				switch change {
				case "model":
					s.Model = "provider/other"
				case "effort":
					s.ThinkingEffort = "high"
				case "revision":
					s.Connection.Revision++
				case "connection":
					s.Connection.ID = uuid.NewString()
				case "owner":
					s.Connection.OwnerID = uuid.NewString()
				}
				return s, err
			})
			if _, err := resolveHostedSelection(ctx, cfg, string(raw)); err == nil {
				t.Fatal("resolver substitution accepted")
			}
		})
	}
}
func TestLocalSelectionRejectsHostedConnectionFields(t *testing.T) {
	s := &Server{}
	connection := &gen.ModelConnectionReference{Id: uuid.New(), Revision: 1}
	if err := s.planningSelection(context.Background(), &gen.PlanningAgentModelSelection{Model: "provider/model", Connection: connection}); err == nil {
		t.Fatal("local planning accepted connection")
	}
	if _, _, err := s.normalizeRunModelSelection(context.Background(), &gen.RunModelSelection{Global: gen.RunModelSelectionEntry{Model: "provider/model", Connection: connection}}); err == nil {
		t.Fatal("local run accepted connection")
	}
}
func TestHostedRunResolvesEntireTupleBeforeCapture(t *testing.T) {
	actor, project := uuid.NewString(), uuid.NewString()
	first, second := uuid.New(), uuid.New()
	cfg := &HostedSelectionConfig{ProjectID: project, Resolver: selectionResolverFunc(func(_ context.Context, a, p string, r access.RequestedSelection) (access.Selection, error) {
		if r.Connection.ID == second.String() {
			return access.Selection{}, errors.New("foreign personal connection")
		}
		return access.Selection{Model: r.Model, Connection: access.Connection{ID: r.Connection.ID, Revision: r.Connection.Revision, OwnerID: a, Scope: "personal", Provider: "provider"}}, nil
	})}
	s := &Server{hostedSelection: cfg}
	roles := map[string]gen.RunModelSelectionEntry{"builder": {Model: "provider/model", Connection: &gen.ModelConnectionReference{Id: second, Revision: 1}}}
	selection := &gen.RunModelSelection{Global: gen.RunModelSelectionEntry{Model: "provider/model", Connection: &gen.ModelConnectionReference{Id: first, Revision: 1}}, RoleOverrides: &roles}
	ctx := context.WithValue(context.Background(), principalContextKey{}, Principal{ActorID: actor})
	if pin, err := s.runAccessSnapshot(ctx, selection); err == nil || pin != nil {
		t.Fatal("partial role selection accepted")
	}
}

func TestHostedAPIEnqueueCapturesAccessAtomically(t *testing.T) {
	ctx := context.Background()
	db := openAPIServerTestDB(t)
	actor, project, connection := uuid.NewString(), uuid.NewString(), uuid.NewString()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err = access.BindProjectTx(ctx, tx, project); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	resolver := selectionResolverFunc(func(_ context.Context, a, p string, r access.RequestedSelection) (access.Selection, error) {
		if a != actor || p != project || r.Connection.ID != connection {
			return access.Selection{}, errors.New("foreign selection")
		}
		return access.Selection{Model: r.Model, ThinkingEffort: r.ThinkingEffort, Connection: access.Connection{ID: r.Connection.ID, Revision: r.Connection.Revision, OwnerID: a, Scope: "personal", Provider: "fixture"}}, nil
	})
	handler, err := NewHandler(ServerConfig{DB: db, ProjectRoot: t.TempDir(), PlannerAgent: &testPlannerAgent{}, ProviderModelCatalog: staticProviderModelCatalog{items: defaultTestProviderModels()}, Authenticator: authenticatorFunc(func(*http.Request) (Principal, error) {
		return Principal{ActorID: actor, RoleID: "member", Method: "session", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}), HostedSelection: &HostedSelectionConfig{ProjectID: project, Resolver: resolver}})
	if err != nil {
		t.Fatal(err)
	}
	defer handler.Close(ctx)
	selection := map[string]any{"global": map[string]any{"model": "fixture/model", "thinking_effort": "medium", "connection": map[string]any{"id": connection, "revision": int64(2)}}}
	run := seedRunViaAPI(t, handler, func(body map[string]any) { body["model_selection"] = selection })
	var raw string
	if err := db.QueryRow(`SELECT snapshot FROM execution_access_runs WHERE run_id=?`, run).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var snapshot access.RunSnapshot
	if err = json.Unmarshal([]byte(raw), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.ActorID != actor || snapshot.ProjectID != project || snapshot.Global.Connection.Revision != 2 {
		t.Fatal("run snapshot differs", snapshot)
	}
	var workOrder string
	if err := db.QueryRow(`SELECT work_order_id FROM workflow_runs WHERE run_id=?`, run).Scan(&workOrder); err != nil {
		t.Fatal(err)
	}
	selection["role_overrides"] = map[string]any{"builder": map[string]any{"model": "fixture/model", "connection": map[string]any{"id": uuid.NewString(), "revision": 1}}}
	response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/runs", idempotencyKey: "hosted-invalid-role", body: map[string]any{"work_order_id": workOrder, "publish_mode": "direct", "model_selection": selection}})
	if response.Code != http.StatusBadRequest {
		t.Fatal(response.Code, response.Body.String())
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM workflow_runs`).Scan(&count); err != nil || count != 1 {
		t.Fatal("invalid selection committed a run", count, err)
	}
	trusted := context.WithValue(ctx, principalContextKey{}, Principal{ActorID: actor})
	modelJSON, _ := json.Marshal(selection["global"])
	session, err := handler.planningService.CreateSession(trusted, "Hosted plan", string(modelJSON))
	if err != nil {
		t.Fatal(err)
	}
	operation, err := handler.planningService.CreateOperation(trusted, session.PlanningSessionID, planning.CreateOperationInput{Type: state.PlanningOperationTypeReply, ReplyMarkdown: "Plan this"})
	if err != nil {
		t.Fatal(err)
	}
	var planningRaw string
	if err := db.QueryRow(`SELECT snapshot FROM execution_access_planning WHERE operation_id=?`, operation.Operation.OperationID).Scan(&planningRaw); err != nil {
		t.Fatal(err)
	}
	var planningSnapshot access.Snapshot
	if err = json.Unmarshal([]byte(planningRaw), &planningSnapshot); err != nil || planningSnapshot.ActorID != actor || planningSnapshot.Selection.Connection.Revision != 2 {
		t.Fatal("planning principal lost", planningSnapshot, err)
	}
	other, err := handler.planningService.CreateSession(trusted, "Other plan", string(modelJSON))
	if err != nil {
		t.Fatal(err)
	}
	foreign := context.WithValue(ctx, principalContextKey{}, Principal{ActorID: uuid.NewString()})
	if _, err := handler.planningService.CreateOperation(foreign, other.PlanningSessionID, planning.CreateOperationInput{Type: state.PlanningOperationTypeReply, ReplyMarkdown: "Cannot inherit"}); err == nil {
		t.Fatal("new actor inherited previous personal connection")
	}
	if err := db.QueryRow(`SELECT count(*) FROM planning_operations WHERE planning_session_id=?`, other.PlanningSessionID).Scan(&count); err != nil || count != 0 {
		t.Fatal("foreign planning operation committed", count, err)
	}
}

func TestHostedSelectionConfigurationRequiresBoundProject(t *testing.T) {
	db := openAPIServerTestDB(t)
	ctx := context.Background()
	project := uuid.NewString()
	resolver := selectionResolverFunc(func(context.Context, string, string, access.RequestedSelection) (access.Selection, error) {
		return access.Selection{}, nil
	})
	cfg := ServerConfig{HostedSelection: &HostedSelectionConfig{ProjectID: project, Resolver: resolver}, Authenticator: authenticatorFunc(func(*http.Request) (Principal, error) { return Principal{}, nil }), ProviderModelCatalog: staticProviderModelCatalog{}}
	if err := validateHostedSelectionConfig(ctx, db, cfg); err == nil {
		t.Fatal("unbound database accepted")
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err = access.BindProjectTx(ctx, tx, project); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := validateHostedSelectionConfig(ctx, db, cfg); err != nil {
		t.Fatal(err)
	}
	for _, alter := range []func(*ServerConfig){func(c *ServerConfig) { c.HostedSelection = nil }, func(c *ServerConfig) { c.Authenticator = nil }, func(c *ServerConfig) { c.ProviderModelCatalog = nil }, func(c *ServerConfig) {
		c.HostedSelection = &HostedSelectionConfig{ProjectID: uuid.NewString(), Resolver: resolver}
	}, func(c *ServerConfig) { c.HostedSelection = &HostedSelectionConfig{ProjectID: project} }} {
		invalid := cfg
		alter(&invalid)
		if err := validateHostedSelectionConfig(ctx, db, invalid); err == nil {
			t.Fatal("incomplete hosted config accepted")
		}
	}
}
