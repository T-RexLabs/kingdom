package api

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	gen "kingdom/internal/apigen"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/planning"
	"kingdom/internal/state"
	"testing"
)

type localResolverFunc func(context.Context, string, string) (access.Selection, error)

func (f localResolverFunc) ResolveLocal(ctx context.Context, m, e string) (access.Selection, error) {
	return f(ctx, m, e)
}
func TestLocalSelectionRejectsForeignCredentialAndChangedTuple(t *testing.T) {
	owner, project := uuid.NewString(), uuid.NewString()
	selection := access.Selection{Model: "fixture/model", ThinkingEffort: "medium", Connection: access.Connection{ID: uuid.NewString(), Revision: 4, OwnerID: owner, Scope: "local_owner", Provider: "fixture"}}
	cfg := &LocalSelectionConfig{OwnerID: owner, ProjectID: project, Resolver: localResolverFunc(func(context.Context, string, string) (access.Selection, error) { return selection, nil })}
	raw := `{"model":"fixture/model","thinking_effort":"medium"}`
	pin, err := resolveLocalSelection(context.Background(), cfg, raw)
	if err != nil || pin.Domain != access.DomainLocalOwner || pin.ActorID != owner {
		t.Fatal(pin, err)
	}
	if _, err := resolveLocalSelection(context.Background(), cfg, `{"model":"fixture/model","thinking_effort":"medium","connection":{"id":"`+selection.Connection.ID+`","revision":4}}`); err == nil {
		t.Fatal("browser credential accepted")
	}
	for _, change := range []func(){func() { selection.Connection.OwnerID = uuid.NewString() }, func() { selection.Connection.Scope = "personal" }, func() { selection.Model = "fixture/other" }, func() { selection.ThinkingEffort = "high" }} {
		saved := selection
		change()
		if _, err := resolveLocalSelection(context.Background(), cfg, raw); err == nil {
			t.Fatal("changed local selection accepted")
		}
		selection = saved
	}
}
func TestLocalManagedEnqueuePinsRevisionWithoutBrowserConnection(t *testing.T) {
	ctx := context.Background()
	db := openAPIServerTestDB(t)
	owner, project, connection := uuid.NewString(), uuid.NewString(), uuid.NewString()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err = providerrequest.BindLocalOwnerTx(ctx, tx, project, owner); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	revision := int64(3)
	cfg := &LocalSelectionConfig{OwnerID: owner, ProjectID: project, Resolver: localResolverFunc(func(_ context.Context, m, e string) (access.Selection, error) {
		return access.Selection{Model: m, ThinkingEffort: e, Connection: access.Connection{ID: connection, Revision: revision, OwnerID: owner, Scope: "local_owner", Provider: "fixture"}}, nil
	})}
	handler, err := NewHandler(ServerConfig{DB: db, OperatorCredential: []byte(apiTestOperatorCredential), ProjectRoot: t.TempDir(), PlannerAgent: &testPlannerAgent{}, ProviderModelCatalog: staticProviderModelCatalog{items: []gen.ProviderModel{{Id: "fixture/model", ProviderId: "fixture", ProviderName: "Fixture", ModelName: "Model", ThinkingEffortOptions: []gen.ThinkingEffortOption{{Value: "medium", Label: "Medium"}}}}}, LocalSelection: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer handler.Close(ctx)
	run := seedRunViaAPI(t, handler, func(body map[string]any) {
		body["model_selection"] = map[string]any{"global": map[string]any{"model": "fixture/model", "thinking_effort": "medium"}}
	})
	var raw string
	if err = db.QueryRow(`SELECT snapshot FROM execution_access_runs WHERE run_id=?`, run).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var pinned access.RunSnapshot
	if err = json.Unmarshal([]byte(raw), &pinned); err != nil || pinned.Domain != access.DomainLocalOwner || pinned.Global.Connection.Revision != 3 {
		t.Fatal(pinned, err)
	}
	session, err := handler.planningService.CreateSession(ctx, "Local plan", `{"model":"fixture/model","thinking_effort":"medium"}`)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := handler.planningService.CreateOperation(ctx, session.PlanningSessionID, planning.CreateOperationInput{Type: state.PlanningOperationTypeReply, ReplyMarkdown: "Plan this"})
	if err != nil {
		t.Fatal(err)
	}
	revision = 9
	if err = db.QueryRow(`SELECT snapshot FROM execution_access_planning WHERE operation_id=?`, operation.Operation.OperationID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var snapshot access.Snapshot
	if err = json.Unmarshal([]byte(raw), &snapshot); err != nil || snapshot.Domain != access.DomainLocalOwner || snapshot.Selection.Connection.Revision != 3 {
		t.Fatal(snapshot, err)
	}
}
