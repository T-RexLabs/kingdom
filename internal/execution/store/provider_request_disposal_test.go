package store

import (
	"context"
	"testing"

	"kingdom/internal/execution/providerrequest"
)

func TestProviderRequestCancellationPublicationRequiresDisposal(t *testing.T) {
	db := openTestDB(t)
	s, req := requestActivationFixture(t, db)
	ctx := context.Background()
	if _, err := s.ActivateProviderInvocation(ctx, req); err != nil {
		t.Fatal(err)
	}
	cancel := QueueAttemptCancelRequest{CancelOutboxID: "request-cancel", AttemptID: req.AttemptID, RunID: req.RunID, NodeRunID: req.NodeRunID, SessionID: req.ProviderSessionID, ReasonCode: "operator_cancelled", RequestedAt: req.OpenedAt, CreatedAt: req.OpenedAt}
	if err := s.QueueAttemptCancel(ctx, cancel); err != nil {
		t.Fatal(err)
	}
	binding, found, err := providerrequest.ForOwner(ctx, db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: req.ProviderInvocationID})
	if err != nil || !found || binding.AdmissionClosedAt == "" {
		t.Fatalf("cancel did not fence queued request: %#v %v", binding, err)
	}
	if err := s.MarkAttemptCancelPublished(ctx, cancel.CancelOutboxID, req.OpenedAt); err == nil {
		t.Fatal("published stop without exact disposal")
	}
	var state string
	if err := db.QueryRow(`SELECT status FROM attempt_cancel_outbox WHERE cancel_outbox_id=?`, cancel.CancelOutboxID).Scan(&state); err != nil || state != "pending" {
		t.Fatalf("failed publication changed outbox: %s %v", state, err)
	}
	if err := s.InvalidateUnlaunchedProviderInvocation(ctx, InvalidateUnlaunchedProviderInvocationRequest{ProviderInvocationID: req.ProviderInvocationID, InvalidReason: "preparation_failed", UpdatedAt: req.OpenedAt, ExpectedLeaseOwner: req.ExpectedLeaseOwner, ExpectedLeaseVersion: req.ExpectedLeaseVersion}); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkAttemptCancelPublished(ctx, cancel.CancelOutboxID, req.OpenedAt); err != nil {
		t.Fatal("explicit undispatched disposal not publishable", err)
	}
}

func TestProviderRequestCancellationLineageFailureKeepsAdmissionOpen(t *testing.T) {
	db := openTestDB(t)
	s, req := requestActivationFixture(t, db)
	ctx := context.Background()
	if _, err := s.ActivateProviderInvocation(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := s.QueueAttemptCancel(ctx, QueueAttemptCancelRequest{CancelOutboxID: "foreign", AttemptID: req.AttemptID, RunID: "other-run", NodeRunID: req.NodeRunID, RequestedAt: req.OpenedAt}); err == nil {
		t.Fatal("foreign lineage accepted")
	}
	b, _, err := providerrequest.ForOwner(ctx, db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: req.ProviderInvocationID})
	if err != nil || b.AdmissionClosedAt != "" {
		t.Fatalf("foreign cancellation changed admission: %#v %v", b, err)
	}
}

func TestProviderRequestObserveTerminalCannotClearAuthority(t *testing.T) {
	db := openTestDB(t)
	s, req := requestActivationFixture(t, db)
	ctx := context.Background()
	if _, err := s.ActivateProviderInvocation(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := s.ObserveTerminal(ctx, ObserveTerminalRequest{OutboxID: "bypass-observation", AttemptID: req.AttemptID, RunID: req.RunID, NodeRunID: req.NodeRunID, TerminalSeq: 1, TerminalState: "committed_failure", PayloadJSON: "{}", ObservedAt: req.OpenedAt}); err == nil {
		t.Fatal("terminal observation cleared bound provider authority")
	}
	attempt, _, err := s.GetAttempt(ctx, req.AttemptID)
	if err != nil || attempt.State != "queued" || attempt.LeaseOwner != req.ExpectedLeaseOwner || attempt.ActiveProviderInvocationID != req.ProviderInvocationID {
		t.Fatalf("rejected observation changed authority: %#v %v", attempt, err)
	}
}

func TestProviderRequestExpiredQueuedClaimCannotStealBoundLease(t *testing.T) {
	db := openTestDB(t)
	s, req := requestActivationFixture(t, db)
	ctx := context.Background()
	if _, err := s.ActivateProviderInvocation(ctx, req); err != nil {
		t.Fatal(err)
	}
	_, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{AttemptID: req.AttemptID, LeaseOwner: "next-worker", LeaseUntil: "2026-03-01T12:30:00Z", ClaimedAt: "2026-03-01T12:20:00Z"})
	if err == nil && acquired {
		t.Fatal("expired bound preparation lease was stolen")
	}
	attempt, _, readErr := s.GetAttempt(ctx, req.AttemptID)
	if readErr != nil || attempt.LeaseOwner != req.ExpectedLeaseOwner || attempt.LeaseVersion != req.ExpectedLeaseVersion {
		t.Fatalf("bound authority changed: %#v %v", attempt, readErr)
	}
}
