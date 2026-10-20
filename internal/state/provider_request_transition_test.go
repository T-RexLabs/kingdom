package state

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	executionstore "kingdom/internal/execution/store"
)

func boundTransitionRequestFixture(t *testing.T) (*sql.DB, liveNodeAttempt, providerrequest.Binding, string) {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	ctx := context.Background()
	now := time.Now().UTC()
	stamp := now.Format(time.RFC3339Nano)
	store := executionstore.New(db)
	const attemptID = "run_1:work_intake:1"
	if err := store.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, CreatedAt: stamp}); err != nil {
		t.Fatal(err)
	}
	claim, acquired, err := store.ClaimQueuedProviderAttempt(ctx, executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "worker", LeaseUntil: now.Add(time.Hour).Format(time.RFC3339Nano), ClaimedAt: stamp})
	if err != nil || !acquired {
		t.Fatalf("claim: %v %v", acquired, err)
	}
	identity := providerexecution.RequestIdentity{InstanceID: "instance", Generation: "generation", Location: "/project", SessionID: "shared-session", ProfileID: "profile", RequestID: "request"}
	_, err = store.ActivateProviderInvocation(ctx, executionstore.ActivateProviderInvocationRequest{ProviderInvocationID: "shared-invocation", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "conversation", ProviderSessionID: identity.SessionID, CompletionMode: executionstore.ProviderCompletionModeServerResponse, IssuedAt: stamp, CreatedAt: stamp, OpenedAt: stamp, ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion, RequestIdentity: identity})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state='running'`)
	b, found, err := providerrequest.ForOwner(ctx, db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: "shared-invocation"})
	if err != nil || !found {
		t.Fatalf("binding: %v %v", found, err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = providerrequest.IssueDispatchTx(ctx, tx, b.ID, b.Identity, b.Lease, now); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return db, liveNodeAttempt{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", NodeID: "work_intake", ExecutionMode: executionstore.ExecutionModeProviderBacked, ExecutionState: "running", ActiveProviderInvocationID: "shared-invocation", ProviderSessionID: identity.SessionID, SessionID: "conversation", LeaseOwner: claim.LeaseOwner, LeaseVersion: claim.LeaseVersion, LeaseExpiresAt: claim.LeaseExpiresAt}, b, stamp
}

func TestRunTerminalizationClosesBoundRequestAdmission(t *testing.T) {
	db, attempt, b, stamp := boundTransitionRequestFixture(t)
	transition := baseTransitionRequest(EntityTypeRun, "running", "cancelled")
	transition.OccurredAt = stamp
	if err := NewTransitionService(db, nil).TerminalizeRunAndCancelLiveNodes(context.Background(), RunTerminalizationRequest{Transition: transition}); err != nil {
		t.Fatal(err)
	}
	current, err := providerrequest.Get(context.Background(), db, b.ID)
	if err != nil || current.AdmissionClosedAt == "" {
		t.Fatalf("cancellation did not fence request admission: %+v %v", current, err)
	}
	actual, _, err := executionstore.New(db).GetAttempt(context.Background(), attempt.AttemptID)
	if err != nil || actual.State != "running" || actual.LeaseOwner != b.Lease.Owner {
		t.Fatalf("cancellation prematurely released request owner: %+v %v", actual, err)
	}
}

func TestStaleRecoveryCannotSupersedeBoundRequestThatAdmittedATool(t *testing.T) {
	db, attempt, _, stamp := boundTransitionRequestFixture(t)
	if _, err := db.Exec(`INSERT INTO provider_native_grants(grant_id,binding_id,message_id,call_id,tool,input_json,identity_digest,created_at) SELECT 'grant',binding_id,'assistant','call','write','{}',?,? FROM provider_requests`, strings.Repeat("a", 64), stamp); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := supersedeStaleProviderInvocationTx(context.Background(), tx, attempt, stamp); err == nil {
		t.Fatal("stale recovery superseded a request with an admitted tool")
	}
}

func TestCancellationHandoffCannotReleaseBoundRequest(t *testing.T) {
	db, attempt, _, stamp := boundTransitionRequestFixture(t)
	store := executionstore.New(db)
	if err := store.QueueAttemptCancel(context.Background(), executionstore.QueueAttemptCancelRequest{CancelOutboxID: "cancel", AttemptID: attempt.AttemptID, RunID: attempt.RunID, NodeRunID: attempt.NodeRunID, SessionID: attempt.SessionID, RequestedAt: stamp, CreatedAt: stamp}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewTransitionService(db, nil).ReleaseFencedStaleRetryAfterCancellation(context.Background(), attempt.AttemptID, stamp); err == nil {
		t.Fatal("cancellation publication released unresolved bound request")
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM attempt_cancel_outbox WHERE cancel_outbox_id='cancel'`).Scan(&status); err != nil || status != "pending" {
		t.Fatalf("failed release changed cancellation: %s %v", status, err)
	}
}
