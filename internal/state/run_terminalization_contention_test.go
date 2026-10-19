package state

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	executionstore "kingdom/internal/execution/store"
	"modernc.org/sqlite"
)

type runTerminalizationWALConnector struct {
	dsn         string
	base        driver.Driver
	beforeWrite func()
}

func (c *runTerminalizationWALConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.base.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &runTerminalizationWALConn{Conn: conn, connector: c}, nil
}

func (c *runTerminalizationWALConnector) Driver() driver.Driver { return c.base }

type runTerminalizationWALConn struct {
	driver.Conn
	connector *runTerminalizationWALConnector
}

func (c *runTerminalizationWALConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Run terminalization reads its live attempts, then releases an agent at
	// the first write. Let a real preparation heartbeat invalidate that WAL
	// snapshot before the underlying driver attempts the write upgrade.
	if strings.HasPrefix(strings.TrimSpace(query), "UPDATE agents") {
		if before := c.connector.beforeWrite; before != nil {
			c.connector.beforeWrite = nil
			before()
		}
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func TestRunTerminalizationRetriesConcurrentPreparationWriter(t *testing.T) {
	for _, scenario := range []struct{ terminal, writer string }{{"cancelled", "heartbeat"}, {"failed", "heartbeat"}, {"cancelled", "provider_activated"}, {"cancelled", "default_timestamp"}} {
		t.Run(scenario.terminal+"/"+scenario.writer, func(t *testing.T) {
			dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)", filepath.Join(t.TempDir(), "cancellation.db"))
			otherDB, err := sql.Open("sqlite", dsn)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { otherDB.Close() })
			applyMigration(t, otherDB, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, otherDB)
			connector := &runTerminalizationWALConnector{dsn: dsn, base: &sqlite.Driver{}}
			db := sql.OpenDB(connector)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() { db.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			store := executionstore.New(otherDB)
			for i := range 4 {
				nodeID, nodeRunID := fmt.Sprintf("advisor_%d", i), fmt.Sprintf("nr_advisor_%d", i)
				if _, err := otherDB.Exec(`INSERT INTO workflow_nodes(workflow_node_id,workflow_definition_id,node_id,required_role,node_class,created_at) VALUES (?,'wfdef_1',?,'governance','advisory_consultation','2026-03-01T12:00:00Z')`, "wn_"+nodeID, nodeID); err != nil {
					t.Fatal(err)
				}
				if _, err := otherDB.Exec(`INSERT INTO node_runs(node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id,row_version) VALUES (?,'run_1',?,1,1,'ready','governance','agent_governance',1)`, nodeRunID, nodeID); err != nil {
					t.Fatal(err)
				}
				attemptID := "run_1:" + nodeID + ":1"
				if err := store.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: nodeRunID, ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, SessionRef: "session_" + nodeID, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
					t.Fatal(err)
				}
				if _, acquired, err := store.ClaimQueuedProviderAttempt(ctx, executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "preparer_" + nodeID, LeaseUntil: "2999-03-01T12:01:00Z", ClaimedAt: "2026-03-01T12:00:00Z"}); err != nil || !acquired {
					t.Fatalf("claim preparation: acquired=%t err=%v", acquired, err)
				}
			}
			interleaved := false
			var writerErr error
			connector.beforeWrite = func() {
				interleaved = true
				if scenario.writer == "provider_activated" {
					_, writerErr = store.ActivateProviderInvocation(ctx, withBoundRequest(executionstore.ActivateProviderInvocationRequest{ProviderInvocationID: "invocation_cancel_race", AttemptID: "run_1:advisor_0:1", RunID: "run_1", NodeRunID: "nr_advisor_0", LogicalSessionID: "session_advisor_0", CompletionMode: executionstore.ProviderCompletionModeServerResponse, ProviderSessionID: "provider_session_cancel_race", CreatedAt: "2026-03-01T12:00:01Z", OpenedAt: "2026-03-01T12:00:01Z", ExpectedLeaseOwner: "preparer_advisor_0", ExpectedLeaseVersion: 1}))
					return
				}
				_, writerErr = otherDB.ExecContext(ctx, `UPDATE attempt_executions SET lease_expires_at='2026-03-01T12:02:00Z' WHERE attempt_id='run_1:advisor_0:1'`)
			}
			transition := baseTransitionRequest(EntityTypeRun, "running", scenario.terminal)
			transition.EventType = "run.state_changed"
			transition.OccurredAt = "2026-03-01T12:00:02Z"
			if scenario.writer == "default_timestamp" {
				transition.OccurredAt = ""
			}
			err = NewTransitionService(db, loadStateTestSchemas(t)).TerminalizeRunAndCancelLiveNodes(ctx, RunTerminalizationRequest{Transition: transition})
			if !interleaved || writerErr != nil {
				t.Fatalf("preparation writer did not commit across cancellation snapshot: interleaved=%t err=%v", interleaved, writerErr)
			}
			if err != nil {
				t.Fatalf("run terminalization must retry invalidated WAL snapshot: %v", err)
			}
			var runStatus string
			if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id='run_1'`).Scan(&runStatus); err != nil || runStatus != scenario.terminal {
				t.Fatalf("run terminalization=%s err=%v", runStatus, err)
			}
			var handoffs, cancelled, events int
			if err := db.QueryRow(`SELECT (SELECT COUNT(*) FROM attempt_cancel_outbox WHERE run_id='run_1' AND status='pending'), (SELECT COUNT(*) FROM attempt_executions WHERE run_id='run_1' AND state='committed_cancelled' AND lease_owner=''), (SELECT COUNT(*) FROM attempt_event_log WHERE run_id='run_1' AND event_type='attempt.cancelled')`).Scan(&handoffs, &cancelled, &events); err != nil {
				t.Fatal(err)
			}
			wantCancelled := 4
			if scenario.writer == "provider_activated" {
				wantCancelled = 3
				attempt, found, err := store.GetAttempt(ctx, "run_1:advisor_0:1")
				if err != nil || !found || attempt.State == "committed_cancelled" || attempt.ActiveProviderInvocationID != "invocation_cancel_race" || attempt.LeaseOwner != "preparer_advisor_0" {
					t.Fatalf("retry did not reload concurrently activated provider authority: %+v found=%t err=%v", attempt, found, err)
				}
			}
			if handoffs != 4 || cancelled != wantCancelled || events != wantCancelled {
				t.Fatalf("retry lost or duplicated durable effects: handoffs=%d cancelled=%d events=%d", handoffs, cancelled, events)
			}
			if scenario.writer == "default_timestamp" {
				var timestamp string
				if err := db.QueryRow(`SELECT updated_at FROM workflow_runs WHERE run_id='run_1'`).Scan(&timestamp); err != nil {
					t.Fatal(err)
				}
				if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
					t.Fatalf("default terminalization time was not normalized: %q %v", timestamp, err)
				}
				var inconsistent int
				if err := db.QueryRow(`SELECT (SELECT COUNT(*) FROM attempt_cancel_outbox WHERE run_id='run_1' AND requested_at<>?), (SELECT COUNT(*) FROM attempt_event_log WHERE run_id='run_1' AND event_type='attempt.cancelled' AND created_at<>?)`, timestamp, timestamp).Scan(&handoffs, &inconsistent); err != nil || handoffs != 0 || inconsistent != 0 {
					t.Fatalf("retry changed normalized terminalization time: handoffs=%d events=%d err=%v", handoffs, inconsistent, err)
				}
			}
		})
	}
}
