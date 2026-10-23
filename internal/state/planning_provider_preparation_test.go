package state

import (
	"context"
	"database/sql"
	"github.com/google/uuid"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"strings"
	"testing"
)

func TestPlanningPreparationFailureAndRecoveryRetainExactOwner(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		t.Run(map[bool]string{false: "failure", true: "recovery"}[recovery], func(t *testing.T) {
			s, op, a := newPlanningRequestActivation(t)
			ctx := context.Background()
			workspace := t.TempDir()
			if _, err := s.db.Exec(`UPDATE planning_sessions SET project_root=? WHERE planning_session_id=?`, workspace, a.PlanningSessionID); err != nil {
				t.Fatal(err)
			}
			inv, err := s.ReservePlanningProviderInvocation(ctx, CreatePlanningProviderInvocationRequest{WorkspacePath: workspace, PlanningProviderInvocationID: a.PlanningProviderInvocationID, PlanningOperationID: a.PlanningOperationID, PlanningSessionID: a.PlanningSessionID, LogicalSessionID: a.LogicalSessionID, CreatedAt: a.CreatedAt, LeaseOwner: a.LeaseOwner, LeaseVersion: a.LeaseVersion})
			if err != nil {
				t.Fatal(err)
			}
			if inv.InvocationState != PlanningProviderInvocationStateIssued || inv.ProviderSessionID != "" {
				t.Fatalf("startup identity: %+v", inv)
			}
			p, err := providerrequest.NewAuthority(s.db, s.leaseNow).Preparation(ctx, inv.PlanningProviderInvocationID)
			if err != nil {
				t.Fatal(err)
			}
			root, err := providerrequest.OpenPreparationWorkspace(ctx, s.db, p)
			if err != nil {
				t.Fatal(err)
			}
			root.Close()
			op.ActiveProviderInvocationID = &inv.PlanningProviderInvocationID
			if recovery {
				op.LeaseVersion++
				op.LeaseOwner = "recovery"
				if _, err = s.db.Exec(`UPDATE planning_operations SET lease_owner=?,lease_version=? WHERE operation_id=?`, op.LeaseOwner, op.LeaseVersion, op.OperationID); err != nil {
					t.Fatal(err)
				}
				a.LeaseOwner = op.LeaseOwner
				a.LeaseVersion = op.LeaseVersion
				if _, err = s.ActivatePlanningProviderInvocation(ctx, a); err == nil {
					t.Fatal("new lease activated old preparation")
				}
			}
			closed, err := s.AbandonPlanningPreparation(ctx, op)
			if err != nil || !closed {
				t.Fatalf("abandon: %t %v", closed, err)
			}
			inv, _, err = s.GetPlanningProviderInvocation(ctx, inv.PlanningProviderInvocationID)
			if err != nil || inv.InvocationState != PlanningProviderInvocationStateSuperseded {
				t.Fatalf("retained invocation: %+v %v", inv, err)
			}
			var evidence int
			if err = s.db.QueryRow(`SELECT count(*) FROM planning_provider_completion_evidence`).Scan(&evidence); err != nil || evidence != 0 {
				t.Fatalf("manufactured evidence %d %v", evidence, err)
			}
		})
	}
}

func TestPlanningPreparationRemoteDebtCannotBeAbandoned(t *testing.T) {
	s, op, a := newPlanningRequestActivation(t)
	ctx := context.Background()
	inv, err := s.ReservePlanningProviderInvocation(ctx, CreatePlanningProviderInvocationRequest{PlanningProviderInvocationID: a.PlanningProviderInvocationID, PlanningOperationID: a.PlanningOperationID, PlanningSessionID: a.PlanningSessionID, LogicalSessionID: a.LogicalSessionID, CreatedAt: a.CreatedAt, LeaseOwner: a.LeaseOwner, LeaseVersion: a.LeaseVersion})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = providerrequest.ReserveRemoteRuntimeTx(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: inv.PlanningProviderInvocationID}, providerrequest.Lease{Owner: a.LeaseOwner, Version: int64(a.LeaseVersion)}, "remote-binding", remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "workspace"}, uuid.NewString(), strings.Repeat("a", 64), s.leaseNow())
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	op.ActiveProviderInvocationID = &inv.PlanningProviderInvocationID
	if closed, err := s.AbandonPlanningPreparation(ctx, op); closed || err == nil {
		t.Fatalf("remote debt abandoned %t %v", closed, err)
	}
	var active sql.NullString
	if err = s.db.QueryRow(`SELECT active_provider_invocation_id FROM planning_operations WHERE operation_id=?`, op.OperationID).Scan(&active); err != nil || active.String != inv.PlanningProviderInvocationID {
		t.Fatal("lost remote owner")
	}
}

func TestPlanningReservedInvocationActivatesWithExactIdentity(t *testing.T) {
	s, _, a := newPlanningRequestActivation(t)
	ctx := context.Background()
	_, err := s.ReservePlanningProviderInvocation(ctx, CreatePlanningProviderInvocationRequest{PlanningProviderInvocationID: a.PlanningProviderInvocationID, PlanningOperationID: a.PlanningOperationID, PlanningSessionID: a.PlanningSessionID, LogicalSessionID: a.LogicalSessionID, CreatedAt: a.CreatedAt, LeaseOwner: a.LeaseOwner, LeaseVersion: a.LeaseVersion})
	if err != nil {
		t.Fatal(err)
	}
	inv, err := s.ActivatePlanningProviderInvocation(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	if inv.InvocationState != PlanningProviderInvocationStateOpen || inv.ProviderSessionID != a.RequestIdentity.SessionID {
		t.Fatalf("activation changed identity: %+v", inv)
	}
}
