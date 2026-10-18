package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/testutil"
)

func requestActivationFixture(t *testing.T, db *sql.DB) (*Store, ActivateProviderInvocationRequest) {
	t.Helper()
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	s.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 4, 0, time.UTC) }
	ctx := context.Background()
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: "request-attempt", RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: ExecutionModeProviderBacked, CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	claim, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{AttemptID: "request-attempt", LeaseOwner: "worker", LeaseUntil: "2026-03-01T12:10:00Z", ClaimedAt: "2026-03-01T12:00:01Z"})
	if err != nil || !acquired {
		t.Fatalf("claim: %v, %v", acquired, err)
	}
	identity := providerexecution.RequestIdentity{InstanceID: "instance", Generation: "generation", Location: "/project", SessionID: "request-session", ProfileID: "profile", RequestID: "request"}
	return s, ActivateProviderInvocationRequest{
		ProviderInvocationID: "request-invocation", AttemptID: "request-attempt", RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical-request", ProviderSessionID: identity.SessionID,
		CompletionMode: ProviderCompletionModeServerResponse, IssuedAt: "2026-03-01T12:00:02Z", CreatedAt: "2026-03-01T12:00:02Z", OpenedAt: "2026-03-01T12:00:02Z",
		ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion, RequestIdentity: &identity,
	}
}

func TestProviderRequestActivationIsAtomicAndIdentityFenced(t *testing.T) {
	for _, mutation := range []string{"none", "session", "lease", "invalid identity", "expired"} {
		t.Run(mutation, func(t *testing.T) {
			db := openTestDB(t)
			s, req := requestActivationFixture(t, db)
			switch mutation {
			case "session":
				req.RequestIdentity.SessionID = "foreign"
			case "lease":
				req.ExpectedLeaseVersion++
			case "invalid identity":
				req.RequestIdentity.ProfileID = ""
			case "expired":
				s.now = func() time.Time { return time.Date(2026, 3, 1, 12, 11, 0, 0, time.UTC) }
			}
			_, err := s.ActivateProviderInvocation(context.Background(), req)
			if mutation != "none" {
				if err == nil {
					t.Fatal("accepted conflicting request activation")
				}
				if countPhase2Table(t, db, "provider_requests") != 0 || countPhase2Table(t, db, "provider_invocations") != 0 {
					t.Fatal("activation left partial ownership")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			binding, found, err := providerrequest.ForOwner(context.Background(), db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: req.ProviderInvocationID})
			if err != nil || !found || binding.Identity != *req.RequestIdentity {
				t.Fatalf("exact binding missing: %+v %v", binding, err)
			}
			if _, err := s.ActivateProviderInvocation(context.Background(), req); err != nil {
				t.Fatalf("exact activation replay: %v", err)
			}
			req.RequestIdentity.ProfileID = "changed"
			if _, err := s.ActivateProviderInvocation(context.Background(), req); err == nil {
				t.Fatal("changed activation callback accepted")
			}
		})
	}
}

func TestProviderRequestActivationCannotAdoptOpenUnboundInvocation(t *testing.T) {
	db := openTestDB(t)
	s, req := requestActivationFixture(t, db)
	identity := req.RequestIdentity
	req.RequestIdentity = nil
	if _, err := s.ActivateProviderInvocation(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	req.RequestIdentity = identity
	if _, err := s.ActivateProviderInvocation(context.Background(), req); err == nil {
		t.Fatal("adopted an already open unbound invocation")
	}
	if countPhase2Table(t, db, "provider_requests") != 0 {
		t.Fatal("created retroactive authority")
	}
}

func TestProviderRequestActivationConfirmsExactBindingAfterCommitError(t *testing.T) {
	connector := testutil.NewSQLiteCommitFaultConnector(":memory:")
	db := connector.OpenDB()
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	mustExec(t, db, `PRAGMA foreign_keys=ON`)
	s, req := requestActivationFixture(t, db)
	connector.FailNextDurableCommit(func() { connector.FailNextQueryContaining("FROM provider_requests") })
	if _, err := s.ActivateProviderInvocation(context.Background(), req); err != nil {
		t.Fatalf("failed exact durable activation readback: %v", err)
	}
	if countPhase2Table(t, db, "provider_requests") != 1 {
		t.Fatal("activation did not retain one binding")
	}
}

func TestProviderSettlementRejectsUnboundRequestObservation(t *testing.T) {
	s, db, req := phase2ProviderSettlementFixture(t)
	req.Settlement.Outcome = ProviderCompletionOutcomeFailed
	req.Terminal.TerminalState = "committed_failure"
	id := req.Settlement.ProviderInvocationID
	req.StreamObservation.Request = &providerexecution.RequestObservation{
		Identity: providerexecution.RequestIdentity{InstanceID: "instance", Generation: "generation", Location: "/project", SessionID: "ses_phase2", ProfileID: "profile", RequestID: "request"},
		Terminal: providerexecution.RequestTerminal{Outcome: providerexecution.RequestOutcomeFailed, Events: providerexecution.ReferenceText(id, "events"), Messages: providerexecution.ReferenceText(id, "messages")},
	}
	if _, err := s.PersistProviderTerminalHandoff(context.Background(), terminalHandoffRequest(req)); err == nil || !strings.Contains(err.Error(), "provider request") {
		t.Fatalf("unbound request handoff was not rejected: %v", err)
	}
	// Recovered completion is another entry point and must be fenced too.
	req.RecoveredGuardQuiescence = true
	_, err := s.SettleProviderCompletionAndTerminal(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "provider request") {
		t.Fatalf("unbound request observation was not rejected at ownership boundary: %v", err)
	}
	if countPhase2Table(t, db, "provider_completion_settlements") != 0 {
		t.Fatal("unbound request settled")
	}
}

func TestProviderRequestCannotBypassActivationOrRollbackDispatch(t *testing.T) {
	for _, action := range []string{"open", "omit identity", "invalidate", "replace", "unlaunched", "dispatched rollback"} {
		t.Run(action, func(t *testing.T) {
			db := openTestDB(t)
			s, req := requestActivationFixture(t, db)
			if _, err := s.ActivateProviderInvocation(context.Background(), req); err != nil {
				t.Fatal(err)
			}
			var err error
			switch action {
			case "open":
				_, err = s.MarkProviderInvocationOpen(context.Background(), MarkProviderInvocationOpenRequest{ProviderInvocationID: req.ProviderInvocationID, AttemptID: req.AttemptID, ProviderSessionID: req.ProviderSessionID, OpenedAt: req.OpenedAt})
			case "omit identity":
				req.RequestIdentity = nil
				_, err = s.ActivateProviderInvocation(context.Background(), req)
			case "invalidate":
				err = s.InvalidateProviderInvocation(context.Background(), InvalidateProviderInvocationRequest{ProviderInvocationID: req.ProviderInvocationID, InvalidReason: "test", UpdatedAt: req.OpenedAt})
			case "replace":
				_, err = s.CreateProviderInvocation(context.Background(), CreateProviderInvocationRequest{ProviderInvocationID: "replacement", AttemptID: req.AttemptID, RunID: req.RunID, NodeRunID: req.NodeRunID, LogicalSessionID: req.LogicalSessionID, CompletionMode: req.CompletionMode, IssuedAt: req.IssuedAt, CreatedAt: req.CreatedAt})
				if err != nil {
					t.Fatal(err)
				}
				err = s.SupersedeProviderInvocation(context.Background(), SupersedeProviderInvocationRequest{ProviderInvocationID: req.ProviderInvocationID, SupersededByInvocationID: "replacement", UpdatedAt: req.OpenedAt})
			case "unlaunched", "dispatched rollback":
				if action == "dispatched rollback" {
					// Canonical dispatch first requires a started attempt. A
					// queued flag alone must never hide an issued request.
					mustExec(t, db, `UPDATE attempt_executions SET state='running'`)
					tx, txErr := db.BeginTx(context.Background(), nil)
					if txErr != nil {
						t.Fatal(txErr)
					}
					if txErr = providerrequest.IssueDispatchTx(context.Background(), tx, req.ProviderInvocationID, *req.RequestIdentity, providerrequest.Lease{Owner: req.ExpectedLeaseOwner, Version: req.ExpectedLeaseVersion}, s.currentTime()); txErr != nil {
						_ = tx.Rollback()
						t.Fatal(txErr)
					}
					if txErr = tx.Commit(); txErr != nil {
						t.Fatal(txErr)
					}
					mustExec(t, db, `UPDATE attempt_executions SET state='queued'`)
				}
				err = s.InvalidateUnlaunchedProviderInvocation(context.Background(), InvalidateUnlaunchedProviderInvocationRequest{ProviderInvocationID: req.ProviderInvocationID, InvalidReason: "test", UpdatedAt: req.OpenedAt, ExpectedLeaseOwner: req.ExpectedLeaseOwner, ExpectedLeaseVersion: req.ExpectedLeaseVersion})
			}
			if action == "unlaunched" {
				if err != nil {
					t.Fatal(err)
				}
				b, found, readErr := providerrequest.ForOwner(context.Background(), db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: req.ProviderInvocationID})
				if readErr != nil || !found || b.AdmissionClosedAt == "" {
					t.Fatalf("rollback did not permanently fence dispatch: %+v %v", b, readErr)
				}
				return
			}
			if err == nil {
				t.Fatal("bypassed provider request ownership")
			}
			invocation, _, readErr := s.GetProviderInvocation(context.Background(), req.ProviderInvocationID)
			if readErr != nil || invocation.InvocationState != ProviderInvocationStateOpen {
				t.Fatalf("failed action changed invocation: %+v %v", invocation, readErr)
			}
		})
	}
}

func TestProviderRequestCannotFinalizeThroughOutboxWithoutSettlement(t *testing.T) {
	db := openTestDB(t)
	s, req := requestActivationFixture(t, db)
	if _, err := s.ActivateProviderInvocation(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO attempt_outbox(outbox_id,attempt_id,run_id,node_run_id,terminal_seq,terminal_state,payload_json,status,created_at,updated_at) VALUES('bypass',?,'run_1','nr_1',1,'committed_failure','{}','pending',?,?)`, req.AttemptID, req.CreatedAt, req.CreatedAt); err != nil {
		t.Fatal(err)
	}
	_, err := s.FinalizeTerminal(context.Background(), FinalizeTerminalRequest{OutboxID: "bypass", AttemptID: req.AttemptID, RunID: req.RunID, NodeRunID: req.NodeRunID, TerminalState: "committed_failure", PayloadJSON: "{}", CommittedAt: req.OpenedAt})
	if err == nil {
		t.Fatal("outbox bypassed bound provider settlement")
	}
	a, _, readErr := s.GetAttempt(context.Background(), req.AttemptID)
	if readErr != nil || a.State != "queued" || a.ActiveProviderInvocationID != req.ProviderInvocationID {
		t.Fatalf("rejected finalization changed authority: %+v %v", a, readErr)
	}
}
