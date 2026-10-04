package state

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"testing"
	"time"
)

func TestSessionManagerLeaseLifecycle(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedSessionTestAgent(t, db)

	metrics := NewSessionLifecycleMetrics()
	manager := NewSessionManager(db, metrics)

	ctx := context.Background()

	created, err := manager.CreateLease(ctx, CreateSessionLeaseRequest{
		SessionID:       "session_1",
		AgentID:         "agent_governance",
		LeaseExpiresAt:  "2026-03-01T12:01:00Z",
		LastHeartbeatAt: "2026-03-01T12:00:30Z",
		CreatedAt:       "2026-03-01T12:00:00Z",
		UpdatedAt:       "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("create session lease: %v", err)
	}
	if created.Status != SessionLeaseStatusActive {
		t.Fatalf("expected active session status, got %q", created.Status)
	}

	renewed, err := manager.RenewLease(ctx, RenewSessionLeaseRequest{
		SessionID:       "session_1",
		LeaseExpiresAt:  "2026-03-01T12:02:00Z",
		LastHeartbeatAt: "2026-03-01T12:01:20Z",
		UpdatedAt:       "2026-03-01T12:01:20Z",
	})
	if err != nil {
		t.Fatalf("renew session lease: %v", err)
	}
	if renewed.LeaseExpiresAt != "2026-03-01T12:02:00Z" {
		t.Fatalf("unexpected renewed lease expiry: %q", renewed.LeaseExpiresAt)
	}

	closed, err := manager.CloseLease(ctx, CloseSessionLeaseRequest{
		SessionID: "session_1",
		UpdatedAt: "2026-03-01T12:01:40Z",
	})
	if err != nil {
		t.Fatalf("close session lease: %v", err)
	}
	if closed.Status != SessionLeaseStatusClosed {
		t.Fatalf("expected closed status after close, got %q", closed.Status)
	}

	snapshot := metrics.Snapshot()
	if snapshot.CreatedCount != 1 {
		t.Fatalf("expected 1 create metric, got %d", snapshot.CreatedCount)
	}
	if snapshot.RenewedCount != 1 {
		t.Fatalf("expected 1 renew metric, got %d", snapshot.RenewedCount)
	}
	if snapshot.ClosedCount != 1 {
		t.Fatalf("expected 1 close metric, got %d", snapshot.ClosedCount)
	}
}

func TestSessionManagerRenewReportsStateConflictWhenLeaseNotActive(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedSessionTestAgent(t, db)

	manager := NewSessionManager(db, NewSessionLifecycleMetrics())
	ctx := context.Background()

	_, err := manager.CreateLease(ctx, CreateSessionLeaseRequest{
		SessionID:       "session_conflict",
		AgentID:         "agent_governance",
		LeaseExpiresAt:  "2026-03-01T12:05:00Z",
		LastHeartbeatAt: "2026-03-01T12:00:30Z",
		CreatedAt:       "2026-03-01T12:00:00Z",
		UpdatedAt:       "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("create session lease: %v", err)
	}

	_, err = manager.CloseLease(ctx, CloseSessionLeaseRequest{
		SessionID: "session_conflict",
		UpdatedAt: "2026-03-01T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("close session lease: %v", err)
	}

	_, err = manager.RenewLease(ctx, RenewSessionLeaseRequest{
		SessionID:       "session_conflict",
		LeaseExpiresAt:  "2026-03-01T12:06:00Z",
		LastHeartbeatAt: "2026-03-01T12:02:00Z",
		UpdatedAt:       "2026-03-01T12:02:00Z",
	})
	if err == nil {
		t.Fatal("expected renew to fail for non-active lease")
	}

	var conflictErr *SessionLeaseStateConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected SessionLeaseStateConflictError, got %T", err)
	}
	if conflictErr.ObservedStatus != SessionLeaseStatusClosed {
		t.Fatalf("expected observed status closed, got %q", conflictErr.ObservedStatus)
	}
}

func TestSessionManagerReactivateLeaseReopensClosedLease(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedSessionTestAgent(t, db)

	manager := NewSessionManager(db, NewSessionLifecycleMetrics())
	ctx := context.Background()

	_, err := manager.CreateLease(ctx, CreateSessionLeaseRequest{
		SessionID:       "session_reactivate",
		AgentID:         "agent_governance",
		LeaseExpiresAt:  "2026-03-01T12:05:00Z",
		LastHeartbeatAt: "2026-03-01T12:00:30Z",
		CreatedAt:       "2026-03-01T12:00:00Z",
		UpdatedAt:       "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("create session lease: %v", err)
	}

	_, err = manager.CloseLease(ctx, CloseSessionLeaseRequest{
		SessionID: "session_reactivate",
		UpdatedAt: "2026-03-01T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("close session lease: %v", err)
	}

	reactivated, err := manager.ReactivateLease(ctx, ReactivateSessionLeaseRequest{
		SessionID:       "session_reactivate",
		AgentID:         "agent_governance",
		LeaseExpiresAt:  "2026-03-01T12:06:00Z",
		LastHeartbeatAt: "2026-03-01T12:02:00Z",
		UpdatedAt:       "2026-03-01T12:02:00Z",
	})
	if err != nil {
		t.Fatalf("reactivate session lease: %v", err)
	}
	if reactivated.Status != SessionLeaseStatusActive {
		t.Fatalf("expected reactivated lease to be active, got %q", reactivated.Status)
	}
	if reactivated.LastHeartbeatAt != "2026-03-01T12:02:00Z" {
		t.Fatalf("expected reactivated heartbeat to be updated, got %q", reactivated.LastHeartbeatAt)
	}
}

func TestSessionManagerReconcileStaleLeasesExpiresAndRecordsRecoveryMetrics(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedSessionTestAgent(t, db)

	metrics := NewSessionLifecycleMetrics()
	manager := NewSessionManager(db, metrics)
	ctx := context.Background()

	_, err := manager.CreateLease(ctx, CreateSessionLeaseRequest{
		SessionID:       "session_stale",
		AgentID:         "agent_governance",
		LeaseExpiresAt:  "2026-03-01T12:30:00Z",
		LastHeartbeatAt: "2026-03-01T12:00:00Z",
		CreatedAt:       "2026-03-01T12:00:00Z",
		UpdatedAt:       "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("create stale candidate lease: %v", err)
	}

	_, err = manager.CreateLease(ctx, CreateSessionLeaseRequest{
		SessionID:       "session_healthy",
		AgentID:         "agent_governance",
		LeaseExpiresAt:  "2026-03-01T12:30:00Z",
		LastHeartbeatAt: "2026-03-01T12:00:20Z",
		CreatedAt:       "2026-03-01T12:00:00Z",
		UpdatedAt:       "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("create healthy lease: %v", err)
	}

	recovered, err := manager.ReconcileStaleLeases(ctx, FindStaleSessionLeasesRequest{
		Now:                     "2026-03-01T12:00:40Z",
		StaleHeartbeatThreshold: 35 * time.Second,
	})
	if err != nil {
		t.Fatalf("reconcile stale leases: %v", err)
	}
	if len(recovered) != 1 {
		t.Fatalf("expected 1 recovered lease, got %d", len(recovered))
	}
	if recovered[0].SessionID != "session_stale" {
		t.Fatalf("unexpected recovered session: %q", recovered[0].SessionID)
	}
	if recovered[0].Status != SessionLeaseStatusExpired {
		t.Fatalf("expected expired status for recovered lease, got %q", recovered[0].Status)
	}

	healthy, err := manager.GetLease(ctx, "session_healthy")
	if err != nil {
		t.Fatalf("get healthy lease: %v", err)
	}
	if healthy.Status != SessionLeaseStatusActive {
		t.Fatalf("expected healthy lease to remain active, got %q", healthy.Status)
	}

	snapshot := metrics.Snapshot()
	if snapshot.ExpiredCount != 1 {
		t.Fatalf("expected 1 expire metric, got %d", snapshot.ExpiredCount)
	}
	if snapshot.RecoveredCount != 1 {
		t.Fatalf("expected 1 recovered metric, got %d", snapshot.RecoveredCount)
	}
	if snapshot.LastExpiryReasonClass != "stale_heartbeat" {
		t.Fatalf("expected stale_heartbeat reason class, got %q", snapshot.LastExpiryReasonClass)
	}
	if len(snapshot.StaleRecoverySeconds) != 1 {
		t.Fatalf("expected 1 stale recovery duration sample, got %d", len(snapshot.StaleRecoverySeconds))
	}
	if math.Abs(snapshot.StaleRecoverySeconds[0]-40.0) > 0.001 {
		t.Fatalf("unexpected stale recovery seconds: %f", snapshot.StaleRecoverySeconds[0])
	}
}

func TestSessionManagerFindStaleLeasesUsesRFC3339NanoChronology(t *testing.T) {
	t.Parallel()

	const now = "2026-09-25T10:00:00.1Z"
	for _, tc := range []struct {
		name         string
		leaseExpires string
		wantStale    bool
	}{
		{name: "expiry after now", leaseExpires: "2026-09-25T10:00:00.11Z"},
		{name: "expiry before now", leaseExpires: "2026-09-25T10:00:00.05Z", wantStale: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openMigratedTestDB(t)
			seedSessionTestAgent(t, db)
			manager := NewSessionManager(db, NewSessionLifecycleMetrics())
			if _, err := manager.CreateLease(context.Background(), CreateSessionLeaseRequest{
				SessionID:       "session_variable_precision",
				AgentID:         "agent_governance",
				LeaseExpiresAt:  tc.leaseExpires,
				LastHeartbeatAt: now,
				CreatedAt:       now,
				UpdatedAt:       now,
			}); err != nil {
				t.Fatalf("create session lease: %v", err)
			}

			stale, err := manager.FindStaleLeases(context.Background(), FindStaleSessionLeasesRequest{
				Now:                     now,
				StaleHeartbeatThreshold: time.Minute,
			})
			if err != nil {
				t.Fatalf("find stale session leases: %v", err)
			}
			if got := len(stale) > 0; got != tc.wantStale {
				t.Fatalf("lease expiry %s stale=%t, want %t (leases=%#v)", tc.leaseExpires, got, tc.wantStale, stale)
			}
		})
	}
}

func TestNormalizeStaleLeaseInputsAppliesDefaultThreshold(t *testing.T) {
	t.Parallel()

	now, threshold, err := normalizeStaleLeaseInputs(FindStaleSessionLeasesRequest{Now: "2026-03-01T12:00:40Z"})
	if err != nil {
		t.Fatalf("normalize stale lease inputs: %v", err)
	}
	if !now.Equal(time.Date(2026, 3, 1, 12, 0, 40, 0, time.UTC)) {
		t.Fatalf("unexpected normalized now: %s", now)
	}
	if threshold != DefaultStaleHeartbeatThreshold {
		t.Fatalf("expected default stale heartbeat threshold %s, got %s", DefaultStaleHeartbeatThreshold, threshold)
	}
}

func seedSessionTestAgent(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
}
