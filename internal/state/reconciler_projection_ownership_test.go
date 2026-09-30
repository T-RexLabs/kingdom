package state

import (
	"context"
	"testing"
	"time"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/retryengine"
	executionstore "kingdom/internal/execution/store"
)

type reconciliationRetryPlannerFunc func(errorcatalog.Code, int) (retryengine.PlanResult, error)

func (f reconciliationRetryPlannerFunc) Plan(code errorcatalog.Code, count int) (retryengine.PlanResult, error) {
	return f(code, count)
}

func TestReconcilerRetryRepairRechecksConcurrentProjection(t *testing.T) {
	for _, takeover := range []string{"retry_recorded", "outbox_queued"} {
		t.Run(takeover, func(t *testing.T) {
			db, svc, now := newAutomaticRetryTestService(t)
			planned := false
			planner := reconciliationRetryPlannerFunc(func(code errorcatalog.Code, count int) (retryengine.PlanResult, error) {
				// Planning happens after the candidate snapshot has been read.
				// Commit the competing owner's action before retry repair writes.
				planned = true
				if takeover == "retry_recorded" {
					if _, scheduled, err := svc.RecordAutomaticWorkOrderRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry)); err != nil || !scheduled {
						t.Fatalf("commit competing retry projection: scheduled=%t err=%v", scheduled, err)
					}
				} else {
					if err := executionstore.New(db).QueueOutbox(context.Background(), executionstore.QueueOutboxRequest{
						OutboxID: "canonical_outbox", AttemptID: "source_attempt", RunID: "run_1", NodeRunID: "nr_1",
						TerminalSeq: 1, TerminalState: "committed_failure", PayloadJSON: `{}`, CreatedAt: now.Format(time.RFC3339Nano),
					}); err != nil {
						t.Fatal(err)
					}
				}
				return fixedDelayRetryPlanner{delay: time.Minute}.Plan(code, count)
			})
			r := NewReconciler(db, nil, planner, nil)
			if err := r.recoverUnprojectedAutomaticWorkOrderRetries(context.Background(), now); err != nil {
				t.Fatalf("stale retry-repair candidate must yield to its current owner: %v", err)
			}
			if !planned {
				t.Fatal("fixture did not enter the captured-candidate interleaving")
			}
			var decisions, attempts int
			if err := db.QueryRow(`SELECT COUNT(*) FROM retry_decisions WHERE source_attempt_id = 'source_attempt'`).Scan(&decisions); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM node_runs WHERE run_id = 'run_1' AND node_id = 'work_intake'`).Scan(&attempts); err != nil {
				t.Fatal(err)
			}
			wantDecisions, wantAttempts, wantState := 1, 2, "failed"
			if takeover == "outbox_queued" {
				wantDecisions, wantAttempts, wantState = 0, 1, "running"
			}
			if decisions != wantDecisions || attempts != wantAttempts || queryNodeStatus(t, db, "nr_1") != wantState {
				t.Fatalf("competing projection ownership changed: decisions=%d attempts=%d node=%s", decisions, attempts, queryNodeStatus(t, db, "nr_1"))
			}
		})
	}
}

func TestReconcilerLeavesPendingOutboxProjectionToItsOwner(t *testing.T) {
	for _, outboxStatus := range []string{"pending", "failed", "published", "absent"} {
		t.Run(outboxStatus, func(t *testing.T) {
			db, _, now := newAutomaticRetryTestService(t)
			if outboxStatus != "absent" {
				if _, err := db.Exec(`INSERT INTO attempt_outbox
					(outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, created_at, updated_at)
					VALUES ('canonical_outbox', 'source_attempt', 'run_1', 'nr_1', 1, 'committed_failure', '{}', ?, ?, ?)`,
					outboxStatus, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
					t.Fatal(err)
				}
			}
			plannerCalls := 0
			r := NewReconciler(db, nil, reconciliationRetryPlannerFunc(func(code errorcatalog.Code, count int) (retryengine.PlanResult, error) {
				plannerCalls++
				return fixedDelayRetryPlanner{delay: time.Minute}.Plan(code, count)
			}), nil)
			if _, err := r.Reconcile(context.Background(), ReconcileRequest{Now: now.Format(time.RFC3339Nano)}); err != nil {
				t.Fatal(err)
			}
			wantState, wantPlans := "failed", 1
			if outboxStatus == "pending" {
				wantState, wantPlans = "running", 0
			}
			if got := queryNodeStatus(t, db, "nr_1"); got != wantState || plannerCalls != wantPlans {
				t.Fatalf("outbox %s: node=%s planner calls=%d, want %s/%d", outboxStatus, got, plannerCalls, wantState, wantPlans)
			}
		})
	}
}
