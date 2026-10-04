package state

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	executionstore "kingdom/internal/execution/store"
)

func TestPreparationTerminalRequiresCurrentLeaseAndClosesSessionAtomically(t *testing.T) {
	for _, scenario := range []string{"current", "replaced", "expired", "retry_replaced"} {
		t.Run(scenario, func(t *testing.T) {
			db := openMigratedTestDB(t)
			seedMinimumGraph(t, db)
			mustExec(t, db, `UPDATE node_runs SET node_status = 'ready', started_at = NULL WHERE node_run_id = 'nr_1'`)
			store := executionstore.New(db)
			ctx := context.Background()
			if err := store.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{AttemptID: "att_prep", RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, SessionRef: "session_prep", CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
				t.Fatal(err)
			}
			claim, acquired, err := store.ClaimQueuedProviderAttempt(ctx, executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: "att_prep", LeaseOwner: "preparer", LeaseUntil: "2026-03-01T12:01:00Z", ClaimedAt: "2026-03-01T12:00:00Z"})
			if err != nil || !acquired {
				t.Fatalf("claim: acquired=%t err=%v", acquired, err)
			}
			mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_prep', 'agent_governance', 'active', '2026-03-01T12:01:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
			if strings.Contains(scenario, "replaced") {
				mustExec(t, db, `UPDATE attempt_executions SET lease_owner = 'replacement', lease_version = lease_version + 1 WHERE attempt_id = 'att_prep'`)
			}
			req := baseTransitionRequest(EntityTypeNode, "ready", "failed")
			req.NodeRunID = sql.NullString{String: "nr_1", Valid: true}
			req.AttemptID, req.EventType, req.ReasonCode = "att_prep", "attempt.failed", "execution_setup_failed"
			req.OccurredAt = "2026-03-01T12:00:30Z"
			if scenario == "expired" {
				req.OccurredAt = "2026-03-01T12:01:00Z"
			}
			svc := NewTransitionService(db, nil)
			if scenario == "retry_replaced" {
				err = svc.RecordDirectExecutionTerminalWithRetry(ctx, DirectExecutionTerminalWithRetryRequest{Transition: req, ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion, Decision: RetryDecisionCreateRequest{Decision: RetryDecisionFailFast, SubjectKind: RetrySubjectKindWorkOrderAttempt, SourceAttemptID: req.AttemptID}})
			} else {
				err = svc.RecordDirectExecutionTerminal(ctx, DirectExecutionTerminalRequest{Transition: req, ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion})
			}
			wantNode, wantSession, wantExecution := "ready", "active", "queued"
			if scenario == "current" {
				if err != nil {
					t.Fatal(err)
				}
				wantNode, wantSession, wantExecution = "failed", "closed", "committed_failure"
			} else if err == nil || !strings.Contains(err.Error(), "lease authority") {
				t.Fatalf("stale preparation result=%v", err)
			}
			var nodeStatus, sessionStatus, executionState string
			if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_1'`).Scan(&nodeStatus); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT status FROM session_leases WHERE session_id = 'session_prep'`).Scan(&sessionStatus); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = 'att_prep'`).Scan(&executionState); err != nil {
				t.Fatal(err)
			}
			if nodeStatus != wantNode || sessionStatus != wantSession || executionState != wantExecution {
				t.Fatalf("node=%s session=%s execution=%s; want %s/%s/%s", nodeStatus, sessionStatus, executionState, wantNode, wantSession, wantExecution)
			}
		})
	}
}
