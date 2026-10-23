package store

import (
	"context"
	"testing"
	"time"
)

func TestPreparationHasDurableInvocationBeforeProviderStartup(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	s.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 1, 0, time.UTC) }
	ctx := context.Background()
	const attemptID = "run_1:work_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "preparer", LeaseUntil: "2026-03-01T12:01:00Z", ClaimedAt: "2026-03-01T12:00:00Z"})
	if err != nil || !ok {
		t.Fatalf("claim: %v", err)
	}
	_, err = s.ReserveProviderInvocation(ctx, ReserveProviderInvocationRequest{CreateProviderInvocationRequest: CreateProviderInvocationRequest{ProviderInvocationID: "startup", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:01Z", IssuedAt: "2026-03-01T12:00:01Z"}, ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion})
	if err != nil {
		t.Fatal(err)
	}
	// Before startup there must be a recoverable owner, not only a process-local ID.
	a, _, err := s.GetAttempt(ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if a.ActiveProviderInvocationID == "" {
		t.Fatalf("preparation lease %s/%d has no durable startup invocation", claim.LeaseOwner, claim.LeaseVersion)
	}
	activation := withBoundRequest(ActivateProviderInvocationRequest{ProviderInvocationID: "startup", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:01Z", OpenedAt: "2026-03-01T12:00:01Z", ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion})
	if _, err = s.ActivateProviderInvocation(ctx, activation); err != nil {
		t.Fatal(err)
	}

}

func TestPreparationAbandonmentAndExpiredReclaim(t *testing.T) {
	for _, mode := range []string{"failure", "fenced_failure", "crash", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)
			s := New(db)
			s.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 1, 0, time.UTC) }
			ctx := context.Background()
			const id = "run_1:work_intake:1"
			if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: id, RunID: "run_1", NodeRunID: "nr_1", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
				t.Fatal(err)
			}
			claim, _, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{AttemptID: id, LeaseOwner: "preparer", LeaseUntil: "2026-03-01T12:01:00Z", ClaimedAt: "2026-03-01T12:00:00Z"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.ReserveProviderInvocation(ctx, ReserveProviderInvocationRequest{CreateProviderInvocationRequest: CreateProviderInvocationRequest{ProviderInvocationID: "startup", AttemptID: id, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:01Z", IssuedAt: "2026-03-01T12:00:01Z"}, ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion})
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "failure", "fenced_failure":
				if mode == "fenced_failure" {
					if _, e := db.Exec(`UPDATE provider_preparations SET admission_closed=1 WHERE invocation_id='startup'`); e != nil {
						t.Fatal(e)
					}
					wrong := s.InvalidateUnlaunchedProviderInvocation(ctx, InvalidateUnlaunchedProviderInvocationRequest{ProviderInvocationID: "startup", InvalidReason: "prepare_failed", UpdatedAt: "2026-03-01T12:00:02Z", ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion + 1})
					if wrong == nil {
						t.Fatal("foreign cleanup lease invalidated fenced preparation")
					}
				}
				err = s.InvalidateUnlaunchedProviderInvocation(ctx, InvalidateUnlaunchedProviderInvocationRequest{ProviderInvocationID: "startup", InvalidReason: "prepare_failed", UpdatedAt: "2026-03-01T12:00:02Z", ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion})
			case "crash":
				var acquired bool
				_, acquired, err = s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{AttemptID: id, LeaseOwner: "replacement", LeaseUntil: "2026-03-01T12:03:00Z", ClaimedAt: "2026-03-01T12:02:00Z"})
				if !acquired {
					t.Fatal("crashed preparation not reclaimed")
				}
			case "cancel":
				err = s.QueueAttemptCancel(ctx, QueueAttemptCancelRequest{CancelOutboxID: "cancel", AttemptID: id, RunID: "run_1", NodeRunID: "nr_1", RequestedAt: "2026-03-01T12:00:02Z"})
			}
			if err != nil {
				t.Fatal(err)
			}
			inv, _, err := s.GetProviderInvocation(ctx, "startup")
			if err != nil || inv.InvocationState != ProviderInvocationStateInvalidated {
				t.Fatalf("owner not retained invalidated %+v %v", inv, err)
			}
			var count int
			if err = db.QueryRow(`SELECT count(*) FROM provider_completion_evidence`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("manufactured evidence %d %v", count, err)
			}
		})
	}
}
