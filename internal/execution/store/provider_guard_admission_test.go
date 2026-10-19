package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"kingdom/internal/execution/providerrequest"
)

func boundRunGuardianFixture(t *testing.T) (*Store, *sql.DB, providerrequest.Binding, AcknowledgeProviderProcessGuardRequest) {
	t.Helper()
	db := openTestDB(t)
	s, activation := requestActivationFixture(t, db)
	if _, err := s.ActivateProviderInvocation(context.Background(), activation); err != nil {
		t.Fatal(err)
	}
	runGuardianExec(t, db, `UPDATE attempt_executions SET state='running' WHERE attempt_id=?`, activation.AttemptID)
	b, found, err := providerrequest.ForOwner(context.Background(), db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: activation.ProviderInvocationID})
	if err != nil || !found {
		t.Fatalf("binding: found=%t %v", found, err)
	}
	ack := AcknowledgeProviderProcessGuardRequest{
		GuardID: "request-command-guard", ProviderInvocationID: activation.ProviderInvocationID,
		AttemptID: activation.AttemptID, RunID: activation.RunID, NodeRunID: activation.NodeRunID, CommandOrdinal: 1,
		Platform: "unix_process_group", GuardianPID: 201, GuardianIdentityToken: "guardian", ChildPID: 202, ChildIdentityToken: "child",
		TreeID: "202", TreeIdentityToken: "tree", QuiescenceCapability: "command-capability", ExpectedLeaseOwner: b.Lease.Owner, ExpectedLeaseVersion: b.Lease.Version,
		StartedAt: "2026-03-01T12:00:03Z", AcknowledgedAt: "2026-03-01T12:00:04Z",
	}
	return s, db, b, ack
}

func runGuardianTransaction(t *testing.T, db *sql.DB, f func(*sql.Tx) error) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := f(tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestBoundRunGuardianRequiresIssuedPendingCommandAdmission(t *testing.T) {
	for _, mode := range []string{"allowed", "undispatched", "unclaimed", "wrong ordinal", "closed", "expired", "wrong session", "stale lease"} {
		t.Run(mode, func(t *testing.T) {
			s, db, b, ack := boundRunGuardianFixture(t)
			ctx, now := context.Background(), s.currentTime()
			if mode != "undispatched" {
				runGuardianTransaction(t, db, func(tx *sql.Tx) error {
					return providerrequest.IssueDispatchTx(ctx, tx, b.ID, b.Identity, b.Lease, now)
				})
			}
			if mode != "undispatched" && mode != "unclaimed" {
				runGuardianTransaction(t, db, func(tx *sql.Tx) error {
					_, err := providerrequest.ClaimCommandTx(ctx, tx, providerrequest.NativeCall{BindingID: b.ID, Identity: b.Identity, MessageID: "message", CallID: "shell", Tool: "shell", Input: []byte(`{"command":"pwd"}`)}, now)
					return err
				})
			}
			switch mode {
			case "wrong ordinal":
				ack.CommandOrdinal = 2
			case "closed":
				runGuardianTransaction(t, db, func(tx *sql.Tx) error {
					return providerrequest.CloseAdmissionTx(ctx, tx, b.ID, b.Identity, b.Lease, now)
				})
			case "expired":
				s.now = func() time.Time { return now.Add(time.Hour) }
			case "wrong session":
				runGuardianExec(t, db, `UPDATE attempt_executions SET provider_session_id='foreign' WHERE attempt_id=?`, ack.AttemptID)
			case "stale lease":
				runGuardianExec(t, db, `UPDATE attempt_executions SET lease_version=lease_version+1 WHERE attempt_id=?`, ack.AttemptID)
			}
			_, err := s.AcknowledgeProviderProcessGuard(ctx, ack)
			if mode == "allowed" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("guard admitted without exact pending command authority")
			}
			if countPhase2Table(t, db, "provider_process_guards") != 0 {
				t.Fatal("rejected admission left orphan guard")
			}
		})
	}
}

func TestBoundRunGuardianExactReplayAfterAdmissionCloses(t *testing.T) {
	s, db, b, ack := boundRunGuardianFixture(t)
	ctx, now := context.Background(), s.currentTime()
	runGuardianTransaction(t, db, func(tx *sql.Tx) error {
		if err := providerrequest.IssueDispatchTx(ctx, tx, b.ID, b.Identity, b.Lease, now); err != nil {
			return err
		}
		_, err := providerrequest.ClaimCommandTx(ctx, tx, providerrequest.NativeCall{BindingID: b.ID, Identity: b.Identity, MessageID: "message", CallID: "shell", Tool: "shell", Input: []byte(`{"command":"pwd"}`)}, now)
		return err
	})
	if _, err := s.AcknowledgeProviderProcessGuard(ctx, ack); err != nil {
		t.Fatal(err)
	}
	runGuardianTransaction(t, db, func(tx *sql.Tx) error {
		return providerrequest.CloseAdmissionTx(ctx, tx, b.ID, b.Identity, b.Lease, now)
	})
	s.now = func() time.Time { return now.Add(time.Hour) }
	if result, err := s.AcknowledgeProviderProcessGuard(ctx, ack); err != nil || !result.Idempotent {
		t.Fatalf("exact acknowledgement replay: %+v %v", result, err)
	}
	changed := ack
	changed.ChildIdentityToken = "different-child"
	if _, err := s.AcknowledgeProviderProcessGuard(ctx, changed); err == nil {
		t.Fatal("changed acknowledgement replay accepted")
	}
	changed = ack
	changed.GuardID = "replacement-guard"
	changed.CommandOrdinal++
	if _, err := s.AcknowledgeProviderProcessGuard(ctx, changed); err == nil {
		t.Fatal("late new guard admitted")
	}
	if countPhase2Table(t, db, "provider_process_guards") != 1 {
		t.Fatal("replay created an extra guard")
	}
	if _, err := s.IngestProviderProcessGuardQuiescence(ctx, providerProcessGuardQuiescenceProof(ack)); err != nil {
		t.Fatal(err)
	}
	if result, err := s.AcknowledgeProviderProcessGuard(ctx, ack); err != nil || !result.Idempotent || result.Guard.Lifecycle != ProviderProcessGuardLifecycleQuiescent {
		t.Fatalf("quiescent receipt replay: %+v %v", result, err)
	}
}

func TestBoundRunNativeOnlyRequestClosureUsesReceipts(t *testing.T) {
	for _, mode := range []string{"closed", "open", "pending native", "pending command"} {
		t.Run(mode, func(t *testing.T) {
			s, db, b, _ := boundRunGuardianFixture(t)
			ctx, now := context.Background(), s.currentTime()
			runGuardianTransaction(t, db, func(tx *sql.Tx) error {
				if err := providerrequest.IssueDispatchTx(ctx, tx, b.ID, b.Identity, b.Lease, now); err != nil {
					return err
				}
				call := providerrequest.NativeCall{BindingID: b.ID, Identity: b.Identity, MessageID: "message", CallID: "call", Tool: "read", Input: []byte(`{"path":"README.md"}`)}
				if mode == "pending command" {
					call.Tool = "shell"
					_, err := providerrequest.ClaimCommandTx(ctx, tx, call, now)
					if err != nil {
						return err
					}
				} else {
					grant, err := providerrequest.AdmitNativeTx(ctx, tx, call, now)
					if err != nil {
						return err
					}
					if mode != "pending native" {
						started := true
						if err := providerrequest.FinishNativeTx(ctx, tx, providerrequest.NativeReceipt{GrantID: grant.ID, Digest: grant.Digest, Outcome: "completed", Started: &started}, now); err != nil {
							return err
						}
					}
				}
				if mode != "open" {
					return providerrequest.CloseAdmissionTx(ctx, tx, b.ID, b.Identity, b.Lease, now)
				}
				return nil
			})
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			err = providerrequest.RequireResourcesClosedTx(ctx, tx, b.ID)
			if (err == nil) != (mode == "closed") {
				t.Fatalf("native resource closure %s: %v", mode, err)
			}
		})
	}
}

func runGuardianExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func TestBoundRunQuiescenceCannotDiscardUnsettledRequestResources(t *testing.T) {
	for _, mode := range []string{"open admission", "pending native", "pending command", "awaiting terminal", "abandoned"} {
		t.Run(mode, func(t *testing.T) {
			s, db, b, ack := boundRunGuardianFixture(t)
			ctx, now := context.Background(), s.currentTime()
			runGuardianTransaction(t, db, func(tx *sql.Tx) error {
				if mode != "abandoned" {
					if err := providerrequest.IssueDispatchTx(ctx, tx, b.ID, b.Identity, b.Lease, now); err != nil {
						return err
					}
				}
				call := providerrequest.NativeCall{BindingID: b.ID, Identity: b.Identity, MessageID: "message", CallID: "call", Tool: "read", Input: []byte(`{"path":"README.md"}`)}
				if mode == "pending native" {
					if _, err := providerrequest.AdmitNativeTx(ctx, tx, call, now); err != nil {
						return err
					}
				}
				if mode == "pending command" {
					call.Tool = "shell"
					if _, err := providerrequest.ClaimCommandTx(ctx, tx, call, now); err != nil {
						return err
					}
				}
				if mode != "open admission" {
					return providerrequest.CloseAdmissionTx(ctx, tx, b.ID, b.Identity, b.Lease, now)
				}
				return nil
			})
			if mode == "abandoned" {
				runGuardianExec(t, db, `UPDATE provider_invocations SET invocation_state='invalidated' WHERE provider_invocation_id=?`, ack.ProviderInvocationID)
			}
			for name, check := range map[string]func() (bool, error){
				"attempt": func() (bool, error) { return s.AttemptProcessGuardsQuiescent(ctx, ack.AttemptID) },
				"run":     func() (bool, error) { return s.RunProviderProcessGuardsQuiescent(ctx, ack.RunID) },
			} {
				quiescent, err := check()
				if err != nil || quiescent != (mode == "abandoned") {
					t.Fatalf("%s %s quiescence=%t: %v", name, mode, quiescent, err)
				}
			}
		})
	}
}
