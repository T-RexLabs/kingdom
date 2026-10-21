package state

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
)

func TestPlanningDeletionRetainsUnboundRemoteCreation(t *testing.T) {
	store, operation := newPlanningOperationTestStore(t)
	now := time.Date(2026, 4, 21, 16, 0, 2, 0, time.UTC)
	const invocation = "remote-planning-invocation"
	if _, err := store.db.Exec(`INSERT INTO planning_provider_invocations(planning_provider_invocation_id,planning_operation_id,planning_session_id,logical_session_id,provider_session_id,invocation_state,completion_mode,opened_at,created_at,updated_at) VALUES(?,?,?,'conversation','pending','open','server_response',?,?,?)`, invocation, operation.OperationID, operation.PlanningSessionID, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE planning_operations SET active_provider_invocation_id=?,provider_session_id='pending' WHERE operation_id=?`, invocation, operation.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE planning_sessions SET current_operation_id=? WHERE planning_session_id=?`, operation.OperationID, operation.PlanningSessionID); err != nil {
		t.Fatal(err)
	}
	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	reservation, err := providerrequest.ReserveRemoteRuntimeTx(context.Background(), tx, providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: invocation}, providerrequest.Lease{Owner: operation.LeaseOwner, Version: int64(operation.LeaseVersion)}, "remote-binding", remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "workspace"}, uuid.NewString(), strings.Repeat("a", 64), now)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	deleted, err := store.DeleteSession(context.Background(), operation.PlanningSessionID)
	if deleted || !errors.Is(err, providerrequest.ErrResourcesOpen) {
		t.Fatalf("unbound remote session deleted=%v err=%v", deleted, err)
	}
	if _, err := remote.ReadRuntime(context.Background(), store.db, reservation.Reference.Allocation); err != nil {
		t.Fatal("reservation lost", err)
	}
	var n int
	if err := store.db.QueryRow(`SELECT count(*) FROM planning_provider_invocations WHERE planning_provider_invocation_id=?`, invocation).Scan(&n); err != nil || n != 1 {
		t.Fatalf("canonical owner lost count=%d err=%v", n, err)
	}
}
