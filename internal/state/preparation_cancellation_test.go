package state

import (
	"context"
	"testing"

	executionstore "kingdom/internal/execution/store"
)

func TestRunTerminalizationQueuesCancellationForAdmittedPreparation(t *testing.T) {
	for _, terminal := range []string{"cancelled", "failed"} {
		for _, preparation := range []string{"unadmitted", "active", "released"} {
			t.Run(terminal+"/"+preparation, func(t *testing.T) {
				db := openTestDB(t)
				applyMigration(t, db, "0001_initial_schema.up.sql")
				seedMinimumGraph(t, db)
				mustExec(t, db, `UPDATE node_runs SET node_status='ready',started_at=NULL WHERE node_run_id='nr_1'`)
				ctx := context.Background()
				store := executionstore.New(db)
				const attemptID = "run_1:decree_intake:1"
				// Preparation can exist before any provider session or invocation.
				if err := store.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
					t.Fatal(err)
				}
				if preparation != "unadmitted" {
					claim, acquired, err := store.ClaimQueuedProviderAttempt(ctx, executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "preparer", LeaseUntil: "2026-03-01T12:01:00Z", ClaimedAt: "2026-03-01T12:00:00Z"})
					if err != nil || !acquired {
						t.Fatalf("claim preparation: acquired=%t err=%v", acquired, err)
					}
					if preparation == "released" {
						if err := store.ReleaseQueuedProviderAttemptLease(ctx, attemptID, claim.LeaseOwner, claim.LeaseVersion, "2026-03-01T12:00:01Z"); err != nil {
							t.Fatal(err)
						}
					}
				}
				transition := baseTransitionRequest(EntityTypeRun, "running", terminal)
				transition.OccurredAt = "2026-03-01T12:00:02Z"
				if err := NewTransitionService(db, nil).TerminalizeRunAndCancelLiveNodes(ctx, RunTerminalizationRequest{Transition: transition}); err != nil {
					t.Fatal(err)
				}
				var count int
				if err := db.QueryRow(`SELECT COUNT(*) FROM attempt_cancel_outbox WHERE attempt_id=? AND status='pending' AND handoff_state='cancellation_requested'`, attemptID).Scan(&count); err != nil {
					t.Fatal(err)
				}
				want := 1
				if preparation == "unadmitted" {
					want = 0
				}
				if count != want {
					t.Fatalf("durable preparation cancellation handoffs=%d want %d", count, want)
				}
				attempt, _, err := store.GetAttempt(ctx, attemptID)
				if err != nil || attempt.State != "committed_cancelled" || attempt.LeaseOwner != "" || attempt.StartedAt != "" || attempt.ActiveProviderInvocationID != "" {
					t.Fatalf("queued cancellation did not revoke launch authority: %+v err=%v", attempt, err)
				}
			})
		}
	}
}

func TestSupersededPreparationRetainsCancellationHandoffWithoutLaunch(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id,row_version) VALUES ('nr_preparing_retry','run_1','decree_intake',2,2,'ready','king','agent_king',1)`)
	ctx := context.Background()
	store := executionstore.New(db)
	const attemptID = "run_1:decree_intake:2"
	if err := store.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_preparing_retry", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if _, acquired, err := store.ClaimQueuedProviderAttempt(ctx, executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "preparer", LeaseUntil: "2026-03-01T12:01:00Z", ClaimedAt: "2026-03-01T12:00:00Z"}); err != nil || !acquired {
		t.Fatalf("claim preparation: acquired=%t err=%v", acquired, err)
	}
	result, err := NewTransitionService(db, nil).SupersedeQueuedAttemptsBeforeLaunch(ctx, SupersedeQueuedAttemptsRequest{RunID: "run_1", NodeID: "decree_intake", KeepAttempt: 1, OccurredAt: "2026-03-01T12:00:02Z", ActorID: "runtime_orchestrator"})
	if err != nil || !result.AllowsCanonicalAdoption() {
		t.Fatalf("supersede preparation: %+v err=%v", result, err)
	}
	var reason string
	if err := db.QueryRow(`SELECT reason_code FROM attempt_cancel_outbox WHERE attempt_id=? AND status='pending'`, attemptID).Scan(&reason); err != nil || reason != "superseded_before_launch" {
		t.Fatalf("superseded preparation lost handoff or falsely claimed launch: reason=%q err=%v", reason, err)
	}
}
