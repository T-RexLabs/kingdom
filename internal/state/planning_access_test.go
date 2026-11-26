package state

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"kingdom/internal/execution/access"
)

func planningAccessFixture(t *testing.T, resolve PlanningAccessResolver) (*PlanningStore, access.Snapshot) {
	t.Helper()
	ctx := context.Background()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	actor := uuid.NewString()
	snapshot := access.Snapshot{Version: 1, ActorID: actor, ProjectID: uuid.NewString(), Selection: access.Selection{Model: "fixture/model", ThinkingEffort: "medium", Connection: access.Connection{ID: uuid.NewString(), Revision: 1, OwnerID: actor, Scope: "personal", Provider: "fixture"}}}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err = access.BindProjectTx(ctx, tx, snapshot.ProjectID); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	store := NewPlanningStore(db, WithPlanningAccessResolver(resolve))
	raw, _ := json.Marshal(access.RequestedSelection{Model: snapshot.Selection.Model, ThinkingEffort: snapshot.Selection.ThinkingEffort, Connection: &access.ConnectionReference{ID: snapshot.Selection.Connection.ID, Revision: 1}})
	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{PlanningSessionID: "session", Title: "Session", TitleState: "custom", LifecycleState: PlanningSessionLifecycleStateActive, ConversationState: PlanningSessionConversationStateReady, ProjectRoot: "/workspace", ModelSelectionJSON: string(raw), CreatedAt: "2026-10-21T12:00:00Z", UpdatedAt: "2026-10-21T12:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	bindTestPlanningContext(t, db, "session")
	return store, snapshot
}
func accessOperation(id string) CreatePlanningOperationRequest {
	return CreatePlanningOperationRequest{OperationID: id, PlanningSessionID: "session", Type: PlanningOperationTypeReply, Status: PlanningOperationStatusQueued, RequestPayloadJSON: `{}`, CreatedAt: "2026-10-21T12:00:01Z"}
}
func TestHostedPlanningAccessFailureRollsBackOperation(t *testing.T) {
	for _, failure := range []string{"missing", "denied", "foreign_project", "foreign_personal", "changed_model", "changed_revision"} {
		t.Run(failure, func(t *testing.T) {
			store, snapshot := planningAccessFixture(t, nil)
			if failure != "missing" {
				store.resolveAccess = func(context.Context, string) (access.Snapshot, error) {
					switch failure {
					case "denied":
						return access.Snapshot{}, errors.New("denied")
					case "foreign_project":
						snapshot.ProjectID = uuid.NewString()
					case "foreign_personal":
						snapshot.ActorID = uuid.NewString()
					case "changed_model":
						snapshot.Selection.Model = "fixture/other"
					case "changed_revision":
						snapshot.Selection.Connection.Revision++
					}
					return snapshot, nil
				}
			}
			if _, err := store.CreateOperation(context.Background(), accessOperation("denied")); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
			for _, table := range []string{"planning_operations", "execution_access_planning"} {
				var count int
				if err := store.db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
					t.Fatal("operation partially committed", table, count, err)
				}
			}
		})
	}
}
func TestHostedPlanningTitleCopiesExactCausalAccess(t *testing.T) {
	ctx := context.Background()
	store, snapshot := planningAccessFixture(t, nil)
	calls := 0
	store.resolveAccess = func(context.Context, string) (access.Snapshot, error) { calls++; return snapshot, nil }
	reply, err := store.CreateOperation(ctx, accessOperation("reply"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE planning_sessions SET model_selection_json='{"model":"another/model"}' WHERE planning_session_id='session'`); err != nil {
		t.Fatal(err)
	}
	store.resolveAccess = func(context.Context, string) (access.Snapshot, error) {
		t.Fatal("automatic title resolved a new actor or connection")
		return access.Snapshot{}, nil
	}
	titleReq := accessOperation("title")
	titleReq.Type = PlanningOperationTypeGenerateTitle
	titleReq.ModelSelectionSourceOperationID = reply.OperationID
	title, err := store.CreateOperation(ctx, titleReq)
	if err != nil {
		t.Fatal(err)
	}
	if title.ModelSelectionJSON != reply.ModelSelectionJSON {
		t.Fatal("causal model changed")
	}
	var source, target string
	if err := store.db.QueryRow(`SELECT snapshot FROM execution_access_planning WHERE operation_id='reply'`).Scan(&source); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT snapshot FROM execution_access_planning WHERE operation_id='title'`).Scan(&target); err != nil {
		t.Fatal(err)
	}
	if source != target || calls != 1 {
		t.Fatal("causal access changed")
	}
}
func TestHostedPlanningNewActionRequiresNewActorResolution(t *testing.T) {
	store, snapshot := planningAccessFixture(t, nil)
	ctx := context.Background()
	store.resolveAccess = func(context.Context, string) (access.Snapshot, error) { return snapshot, nil }
	if _, err := store.CreateOperation(ctx, accessOperation("first")); err != nil {
		t.Fatal(err)
	}
	store.resolveAccess = func(context.Context, string) (access.Snapshot, error) {
		return access.Snapshot{}, errors.New("personal connection belongs to another actor")
	}
	if _, err := store.CreateOperation(ctx, accessOperation("second")); err == nil {
		t.Fatal("new action inherited previous actor")
	}
}

func TestHostedPlanningTitleRejectsMissingCausalSnapshot(t *testing.T) {
	store, _ := planningAccessFixture(t, nil)
	// A historical or incomplete row must not borrow the current session's access.
	if _, err := store.db.Exec(`INSERT INTO planning_operations(operation_id,acp_run_id,planning_session_id,type,status,request_payload_json,model_selection_json,created_at) SELECT 'source','acp-source',planning_session_id,'reply','succeeded','{}',model_selection_json,'2026-10-21T12:00:01Z' FROM planning_sessions WHERE planning_session_id='session'`); err != nil {
		t.Fatal(err)
	}
	req := accessOperation("title")
	req.Type = PlanningOperationTypeGenerateTitle
	req.ModelSelectionSourceOperationID = "source"
	if _, err := store.CreateOperation(context.Background(), req); err == nil {
		t.Fatal("missing causal access fell back to session")
	}
	var count int
	if err := store.db.QueryRow(`SELECT count(*) FROM planning_operations WHERE operation_id='title'`).Scan(&count); err != nil || count != 0 {
		t.Fatal("title partially committed", count, err)
	}
}
