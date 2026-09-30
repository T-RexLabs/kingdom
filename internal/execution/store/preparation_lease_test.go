package store

import (
	"context"
	"testing"
)

func TestQueuedPreparationLeaseRenewsWithoutAdvancingLifecycle(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	claim, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "preparation-worker", LeaseUntil: "2026-03-01T12:01:00Z", ClaimedAt: "2026-03-01T12:00:00Z"})
	if err != nil || !acquired {
		t.Fatalf("claim: acquired=%t err=%v", acquired, err)
	}
	renewed, err := s.RenewProviderAttemptLease(ctx, RenewProviderAttemptLeaseRequest{AttemptID: attemptID, LeaseOwner: claim.LeaseOwner, LeaseVersion: claim.LeaseVersion, LeaseUntil: "2026-03-01T12:02:00Z", RenewedAt: "2026-03-01T12:00:30Z"})
	if err != nil || !renewed {
		t.Fatalf("queued preparation lost its lease: renewed=%t err=%v", renewed, err)
	}
	attempt, found, err := s.GetAttempt(ctx, attemptID)
	if err != nil || !found || attempt.State != "queued" || attempt.LaunchStartedAt != "" || attempt.StartedAt != "" || attempt.LeaseVersion != claim.LeaseVersion || attempt.LeaseExpiresAt != "2026-03-01T12:02:00Z" {
		t.Fatalf("preparation renewal changed lifecycle or lineage: found=%t err=%v attempt=%#v", found, err, attempt)
	}
}

func TestReleasePreparationLeasePreservesForeignGenerationAndActiveExecution(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	claim, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "preparer", LeaseUntil: "2026-03-01T12:01:00Z", ClaimedAt: "2026-03-01T12:00:00Z"})
	if err != nil || !acquired {
		t.Fatalf("claim: acquired=%t err=%v", acquired, err)
	}
	for _, fence := range []struct {
		owner   string
		version int64
	}{{"other", claim.LeaseVersion}, {claim.LeaseOwner, claim.LeaseVersion + 1}} {
		if err := s.ReleaseQueuedProviderAttemptLease(ctx, attemptID, fence.owner, fence.version, "2026-03-01T12:00:30Z"); err != nil {
			t.Fatal(err)
		}
		a, _, err := s.GetAttempt(ctx, attemptID)
		if err != nil || a.LeaseOwner != claim.LeaseOwner {
			t.Fatalf("foreign release changed claim: %#v err=%v", a, err)
		}
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET state = 'starting' WHERE attempt_id = ?`, attemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseQueuedProviderAttemptLease(ctx, attemptID, claim.LeaseOwner, claim.LeaseVersion, "2026-03-01T12:00:30Z"); err != nil {
		t.Fatal(err)
	}
	a, _, err := s.GetAttempt(ctx, attemptID)
	if err != nil || a.LeaseOwner != claim.LeaseOwner {
		t.Fatalf("active release changed claim: %#v err=%v", a, err)
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET state = 'queued' WHERE attempt_id = ?`, attemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseQueuedProviderAttemptLease(ctx, attemptID, claim.LeaseOwner, claim.LeaseVersion, "2026-03-01T12:00:30Z"); err != nil {
		t.Fatal(err)
	}
	a, _, err = s.GetAttempt(ctx, attemptID)
	if err != nil || a.LeaseOwner != "" || a.LeaseExpiresAt != "" || a.LeaseVersion != claim.LeaseVersion || a.State != "queued" {
		t.Fatalf("unlaunched release: %#v err=%v", a, err)
	}
}

func TestUnlaunchedInvocationRollbackRetainsOnlyItsPreparationLease(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	claim, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "preparer", LeaseUntil: "2026-03-01T12:01:00Z", ClaimedAt: "2026-03-01T12:00:00Z"})
	if err != nil || !acquired {
		t.Fatalf("claim: acquired=%t err=%v", acquired, err)
	}
	_, err = s.ActivateProviderInvocation(ctx, ActivateProviderInvocationRequest{ProviderInvocationID: "invocation_prepare", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:01Z", OpenedAt: "2026-03-01T12:00:01Z", ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion})
	if err != nil {
		t.Fatal(err)
	}
	req := InvalidateUnlaunchedProviderInvocationRequest{ProviderInvocationID: "invocation_prepare", InvalidReason: "execution_start_persistence_failed", UpdatedAt: "2026-03-01T12:00:02Z", ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion + 1}
	if err := s.InvalidateUnlaunchedProviderInvocation(ctx, req); err == nil {
		t.Fatal("another lease generation invalidated invocation")
	}
	invocation, _, err := s.GetProviderInvocation(ctx, req.ProviderInvocationID)
	if err != nil || invocation.InvocationState != ProviderInvocationStateOpen {
		t.Fatalf("foreign rollback changed invocation: %#v err=%v", invocation, err)
	}
	req.ExpectedLeaseVersion = claim.LeaseVersion
	if err := s.InvalidateUnlaunchedProviderInvocation(ctx, req); err != nil {
		t.Fatal(err)
	}
	a, _, err := s.GetAttempt(ctx, attemptID)
	if err != nil || a.LeaseOwner != claim.LeaseOwner || a.LeaseVersion != claim.LeaseVersion || a.LeaseExpiresAt != claim.LeaseExpiresAt || a.State != "queued" {
		t.Fatalf("rollback lost preparation fence: %#v err=%v", a, err)
	}
	invocation, _, err = s.GetProviderInvocation(ctx, req.ProviderInvocationID)
	if err != nil || invocation.InvocationState != ProviderInvocationStateInvalidated {
		t.Fatalf("owned rollback: %#v err=%v", invocation, err)
	}
}
