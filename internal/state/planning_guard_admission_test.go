package state

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"kingdom/internal/execution/providerrequest"
)

func planningGuardianTransaction(t *testing.T, db *sql.DB, f func(*sql.Tx) error) {
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

func boundPlanningGuardianFixture(t *testing.T) (*PlanningStore, providerrequest.Binding, PlanningProcessGuardOwnership, time.Time) {
	t.Helper()
	s, db, _, _, ack, now := newPlanningProcessGuardCommitFixture(t)
	b := providerrequest.Binding{
		ID: ack.PlanningProviderInvocationID, Owner: providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: ack.PlanningProviderInvocationID},
		Identity: planningRequestIdentity(), Lease: providerrequest.Lease{Owner: ack.LeaseOwner, Version: int64(ack.LeaseVersion)},
	}
	planningGuardianTransaction(t, db, func(tx *sql.Tx) error { return providerrequest.BindTx(context.Background(), tx, b, now) })
	return s, b, ack, now
}

func TestBoundPlanningGuardianRequiresIssuedPendingCommandAdmission(t *testing.T) {
	for _, mode := range []string{"allowed", "undispatched", "unclaimed", "wrong ordinal", "closed", "expired", "wrong session", "stale lease", "replaced operation"} {
		t.Run(mode, func(t *testing.T) {
			s, b, ack, now := boundPlanningGuardianFixture(t)
			ctx := context.Background()
			if mode != "undispatched" {
				planningGuardianTransaction(t, s.db, func(tx *sql.Tx) error {
					return providerrequest.IssueDispatchTx(ctx, tx, b.ID, b.Identity, b.Lease, now)
				})
			}
			if mode != "undispatched" && mode != "unclaimed" {
				planningGuardianTransaction(t, s.db, func(tx *sql.Tx) error {
					_, err := providerrequest.ClaimCommandTx(ctx, tx, providerrequest.NativeCall{BindingID: b.ID, Identity: b.Identity, MessageID: "message", CallID: "shell", Tool: "shell", Input: []byte(`{"command":"pwd"}`)}, now)
					return err
				})
			}
			var err error
			switch mode {
			case "wrong ordinal":
				ack.CommandOrdinal = 2
			case "closed":
				planningGuardianTransaction(t, s.db, func(tx *sql.Tx) error {
					return providerrequest.CloseAdmissionTx(ctx, tx, b.ID, b.Identity, b.Lease, now)
				})
			case "expired":
				_, err = s.db.Exec(`UPDATE planning_operations SET lease_expires_at=? WHERE operation_id=?`, now.Add(-time.Second).Format(time.RFC3339Nano), ack.PlanningOperationID)
			case "wrong session":
				_, err = s.db.Exec(`UPDATE planning_operations SET provider_session_id='foreign' WHERE operation_id=?`, ack.PlanningOperationID)
			case "stale lease":
				_, err = s.db.Exec(`UPDATE planning_operations SET lease_version=lease_version+1 WHERE operation_id=?`, ack.PlanningOperationID)
			case "replaced operation":
				_, err = s.db.Exec(`UPDATE planning_sessions SET current_operation_id=NULL WHERE planning_session_id=?`, ack.PlanningSessionID)
			}
			if err != nil {
				t.Fatal(err)
			}
			err = s.AcknowledgePlanningProcessGuard(ctx, ack)
			if mode == "allowed" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("guard admitted without exact pending command authority")
			}
			var count int
			if err := s.db.QueryRow(`SELECT COUNT(*) FROM provider_process_guards`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatal("rejected admission left orphan guard")
			}
		})
	}
}

func TestBoundPlanningGuardianExactReplayAfterAdmissionCloses(t *testing.T) {
	s, b, ack, now := boundPlanningGuardianFixture(t)
	ctx := context.Background()
	planningGuardianTransaction(t, s.db, func(tx *sql.Tx) error {
		if err := providerrequest.IssueDispatchTx(ctx, tx, b.ID, b.Identity, b.Lease, now); err != nil {
			return err
		}
		_, err := providerrequest.ClaimCommandTx(ctx, tx, providerrequest.NativeCall{BindingID: b.ID, Identity: b.Identity, MessageID: "message", CallID: "shell", Tool: "shell", Input: []byte(`{"command":"pwd"}`)}, now)
		return err
	})
	if err := s.AcknowledgePlanningProcessGuard(ctx, ack); err != nil {
		t.Fatal(err)
	}
	planningGuardianTransaction(t, s.db, func(tx *sql.Tx) error {
		return providerrequest.CloseAdmissionTx(ctx, tx, b.ID, b.Identity, b.Lease, now)
	})
	if _, err := s.db.Exec(`UPDATE planning_operations SET lease_expires_at=? WHERE operation_id=?`, now.Add(-time.Second).Format(time.RFC3339Nano), ack.PlanningOperationID); err != nil {
		t.Fatal(err)
	}
	if err := s.AcknowledgePlanningProcessGuard(ctx, ack); err != nil {
		t.Fatalf("exact acknowledgement replay: %v", err)
	}
	changed := ack
	changed.ChildIdentityToken = "different-child"
	if err := s.AcknowledgePlanningProcessGuard(ctx, changed); err == nil {
		t.Fatal("changed acknowledgement replay accepted")
	}
	changed = ack
	changed.GuardID = "replacement-guard"
	changed.CommandOrdinal++
	if err := s.AcknowledgePlanningProcessGuard(ctx, changed); err == nil {
		t.Fatal("late new guard admitted")
	}
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM provider_process_guards`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("replay guard count=%d: %v", count, err)
	}
}
