package orchestration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/state"
)

// Each observation advances this clock, so assertions compare the persisted
// causal order without sleeps or assumptions about host performance.
func completionTimestampFixture(t *testing.T) (*orchestrationLoop, asyncExecutionResult) {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	clock := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	loop := &orchestrationLoop{
		db: db, transition: state.NewTransitionService(db, nil),
		mailbox: state.NewMailboxStore(db, nil), events: state.NewAttemptEventIngestor(db, nil),
		executionStore: executionstore.New(db),
		now: func() time.Time {
			clock = clock.Add(time.Second)
			return clock
		},
	}
	result := asyncExecutionResult{
		run:  runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "approved"},
		node: nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
		attempt: nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, Wave: 1,
			AgentID: "agent_governance", RequiredRole: "governance", ExecutionMode: executionstore.ExecutionModeRuntimeOwned},
		attemptID: "run_1:work_intake:1", delegator: "agent_governance", outputText: "Complete.",
		occurredAt: "2026-09-30T11:59:00Z",
	}
	seedCompletionTimestampOutbox(t, db, result)
	return loop, result
}

func seedCompletionTimestampOutbox(t *testing.T, db *sql.DB, result asyncExecutionResult) {
	t.Helper()
	store := executionstore.New(db)
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID: result.attemptID, RunID: result.run.RunID, NodeRunID: result.attempt.NodeRunID,
		ExecutionMode: executionstore.ExecutionModeRuntimeOwned, CompletionMode: executionstore.ProviderCompletionModeNotApplicable,
		CreatedAt: result.occurredAt,
	}); err != nil {
		t.Fatal(err)
	}
	created, err := time.Parse(time.RFC3339Nano, result.occurredAt)
	if err != nil {
		t.Fatal(err)
	}
	loop := orchestrationLoop{db: db, executionStore: store, now: func() time.Time { return created }}
	if err := loop.commitExecutionCompletion(context.Background(), result); err != nil {
		t.Fatal(err)
	}
}

func completionObservationTime(t *testing.T, db *sql.DB, attemptID, phase string) time.Time {
	t.Helper()
	var raw string
	if err := db.QueryRow(`SELECT created_at FROM events WHERE attempt_id = ?
		AND reason_code = 'runtime_completion_observation' AND json_extract(payload_json, '$.phase') = ?
		ORDER BY seq DESC LIMIT 1`, attemptID, phase).Scan(&raw); err != nil {
		t.Fatalf("read %s for %s: %v", phase, attemptID, err)
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestCompletionTimestampsFollowPerRecordProjection(t *testing.T) {
	loop, first := completionTimestampFixture(t)
	mustExec(t, loop.db, `INSERT INTO workflow_nodes
		(workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at)
		VALUES ('wn_2', 'wfdef_1', 'second_intake', 'governance', 'work_intake', '2026-09-30T11:59:00Z')`)
	mustExec(t, loop.db, `INSERT INTO node_runs
		(node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at)
		VALUES ('nr_2', 'run_1', 'second_intake', 1, 1, 'running', 'governance', 'agent_governance', 1, '2026-09-30T11:59:00Z')`)
	second := first
	second.node.NodeID = "second_intake"
	second.attempt.NodeID = "second_intake"
	second.attempt.NodeRunID = "nr_2"
	second.attemptID = "run_1:second_intake:1"
	second.occurredAt = "2026-09-30T11:59:01Z"
	seedCompletionTimestampOutbox(t, loop.db, second)

	if processed, err := loop.drainExecutionOutbox(context.Background()); err != nil || processed != 2 {
		t.Fatalf("drain: processed=%d err=%v", processed, err)
	}
	var previousPublished time.Time
	for _, result := range []asyncExecutionResult{first, second} {
		dequeued := completionObservationTime(t, loop.db, result.attemptID, "exec_outbox_dequeued")
		entered := completionObservationTime(t, loop.db, result.attemptID, "complete_execution_entered")
		exited := completionObservationTime(t, loop.db, result.attemptID, "complete_execution_exited")
		var raw string
		if err := loop.db.QueryRow(`SELECT published_at FROM attempt_outbox WHERE attempt_id = ?`, result.attemptID).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		published, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			t.Fatal(err)
		}
		if !dequeued.After(previousPublished) || !entered.After(dequeued) || !exited.After(entered) || !published.After(exited) {
			t.Errorf("projection timeline must follow effects for %s: previous published=%s dequeue=%s entry=%s exit=%s published=%s",
				result.attemptID, previousPublished, dequeued, entered, exited, published)
		}
		previousPublished = published
	}
}

func TestCompletionDequeuePrecedesPayloadHydrationFailure(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(fmt.Sprintf("malformed_json_%t", malformed), func(t *testing.T) {
			loop, result := completionTimestampFixture(t)
			raw := "{"
			if !malformed {
				var payload executionOutboxPayload
				var stored string
				if err := loop.db.QueryRow(`SELECT payload_json FROM attempt_outbox WHERE attempt_id = ?`, result.attemptID).Scan(&stored); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(stored), &payload); err != nil {
					t.Fatal(err)
				}
				ref := providerexecution.ReferenceText("different_invocation", "missing content")
				payload.OutputTextRef = &ref
				encoded, err := json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
				raw = string(encoded)
			}
			if _, err := loop.db.Exec(`UPDATE attempt_outbox SET payload_json = ? WHERE attempt_id = ?`, raw, result.attemptID); err != nil {
				t.Fatal(err)
			}
			if processed, err := loop.drainExecutionOutbox(context.Background()); err != nil || processed != 0 {
				t.Fatalf("drain invalid payload: processed=%d err=%v", processed, err)
			}
			dequeued := completionObservationTime(t, loop.db, result.attemptID, "exec_outbox_dequeued")
			failed := completionObservationTime(t, loop.db, result.attemptID, "exec_outbox_dead_lettered")
			if !failed.After(dequeued) {
				t.Fatalf("dead-letter time %s must follow dequeue %s", failed, dequeued)
			}
			var status, updated string
			if err := loop.db.QueryRow(`SELECT status, updated_at FROM attempt_outbox WHERE attempt_id = ?`, result.attemptID).Scan(&status, &updated); err != nil {
				t.Fatal(err)
			}
			if status != "failed" || updated != failed.Format(time.RFC3339Nano) {
				t.Fatalf("dead-letter persistence must reflect failure time: status=%s updated=%s failure=%s", status, updated, failed)
			}
		})
	}
}

func TestCompletionProjectionReplayRetainsObservationsWithoutRepeatingCanonicalEvents(t *testing.T) {
	loop, result := completionTimestampFixture(t)
	// A frozen clock also ensures replay identity does not depend on clock precision.
	loop.now = func() time.Time { return time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC) }
	mustExec(t, loop.db, `CREATE TRIGGER interrupt_publication BEFORE UPDATE OF status ON attempt_outbox
		WHEN NEW.status = 'published' BEGIN SELECT RAISE(ABORT, 'publication interrupted'); END`)
	if _, err := loop.drainExecutionOutbox(context.Background()); err == nil || !strings.Contains(err.Error(), "publication interrupted") {
		t.Fatalf("expected interruption after successful projection, got %v", err)
	}
	mustExec(t, loop.db, `DROP TRIGGER interrupt_publication`)
	if processed, err := loop.drainExecutionOutbox(context.Background()); err != nil || processed != 1 {
		t.Fatalf("replay: processed=%d err=%v", processed, err)
	}
	for _, phase := range []string{"exec_outbox_dequeued", "complete_execution_entered", "complete_execution_exited"} {
		var count int
		if err := loop.db.QueryRow(`SELECT COUNT(*) FROM events WHERE attempt_id = ?
			AND reason_code = 'runtime_completion_observation' AND json_extract(payload_json, '$.phase') = ?`, result.attemptID, phase).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Errorf("expected both projection observations for %s, got %d", phase, count)
		}
	}
	for _, query := range []string{
		`SELECT COUNT(*) FROM events WHERE attempt_id = ? AND event_type = 'attempt.succeeded'`,
		`SELECT COUNT(*) FROM mailbox_messages WHERE json_extract(payload_json, '$.attempt_id') = ? AND json_extract(payload_json, '$.communication_type') = 'response_received'`,
	} {
		var count int
		if err := loop.db.QueryRow(query, result.attemptID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("canonical projection duplicated or missing: count=%d query=%s", count, query)
		}
	}
}

func TestCompletionExitRecordsProjectionFailureTime(t *testing.T) {
	loop, result := completionTimestampFixture(t)
	// A valid replay payload whose completion mode disagrees with durable authority
	// must remain retryable; its exit observation must describe the failed pass.
	mustExec(t, loop.db, `UPDATE attempt_outbox SET payload_json = json_set(payload_json, '$.completion_mode', 'unsupported')`)
	if processed, err := loop.drainExecutionOutbox(context.Background()); err != nil || processed != 0 {
		t.Fatalf("failed projection: processed=%d err=%v", processed, err)
	}
	entered := completionObservationTime(t, loop.db, result.attemptID, "complete_execution_entered")
	exited := completionObservationTime(t, loop.db, result.attemptID, "complete_execution_exited")
	failed := completionObservationTime(t, loop.db, result.attemptID, "exec_outbox_retryable_error")
	if !exited.After(entered) || !failed.After(exited) {
		t.Fatalf("failed pass timeline: entry=%s exit=%s retryable=%s", entered, exited, failed)
	}
	var recordedError, status, updated string
	if err := loop.db.QueryRow(`SELECT json_extract(payload_json, '$.error') FROM events
		WHERE attempt_id = ? AND json_extract(payload_json, '$.phase') = 'complete_execution_exited'`, result.attemptID).Scan(&recordedError); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(recordedError, "does not match persisted attempt completion mode") {
		t.Fatalf("expected projection error on exit, got %q", recordedError)
	}
	if err := loop.db.QueryRow(`SELECT status, updated_at FROM attempt_outbox WHERE attempt_id = ?`, result.attemptID).Scan(&status, &updated); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || updated != failed.Format(time.RFC3339Nano) {
		t.Fatalf("retryable persistence must reflect failure time: status=%s updated=%s failure=%s", status, updated, failed)
	}
}

func TestCompletionLoopCapsPreserveZeroAndUnlimitedModes(t *testing.T) {
	for _, gate := range []struct{ class, role, reason string }{
		{"execution_review", "reviewer", "execution_review_cap_auto_succeeded"},
		{"validation_gates", "qa-validator", "validation_gate_cap_auto_succeeded"},
	} {
		for _, unlimited := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/unlimited_%t", gate.class, unlimited), func(t *testing.T) {
				db := openTestDB(t)
				applyMigration(t, db)
				seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
					RunID: "run_cap", WorkOrderID: "work_order_cap", WorkflowID: "workflow_cap",
					NodeRunID: "node_run_cap", NodeID: "custom_gate", NodeClass: gate.class,
					RequiredRole: gate.role, AgentID: "agent_gate", RoleIDs: []string{gate.role},
				})
				if _, err := db.Exec(`UPDATE workflow_runs SET max_execution_review_cycles = 0,
					max_validation_rework_cycles = 0, run_until_all_approve = ? WHERE run_id = 'run_cap'`, unlimited); err != nil {
					t.Fatal(err)
				}
				loop := orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil),
					events: state.NewAttemptEventIngestor(db, nil), executionStore: executionstore.New(db),
					now: func() time.Time { return time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC) }}
				autoSucceeded, err := loop.maybeAutoSucceedLoopCappedReadyNode(context.Background(),
					runSnapshot{RunID: "run_cap", RunStatus: "running", PlanState: "approved"},
					nodeDefinition{NodeID: "custom_gate", RequiredRole: gate.role, NodeClass: gate.class},
					nodeAttempt{NodeRunID: "node_run_cap", NodeID: "custom_gate", Attempt: 1, Wave: 1, RequiredRole: gate.role, AgentID: "agent_gate"},
					"2026-09-30T12:00:00Z")
				if err != nil || autoSucceeded == unlimited {
					t.Fatalf("zero-cap auto-success: unlimited=%t autoSucceeded=%t err=%v", unlimited, autoSucceeded, err)
				}
				processed, err := loop.drainExecutionOutbox(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				var status string
				if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'node_run_cap'`).Scan(&status); err != nil {
					t.Fatal(err)
				}
				if unlimited {
					if status != "ready" || processed != 0 {
						t.Fatalf("unlimited mode must leave the gate ready for provider dispatch: status=%s processed=%d", status, processed)
					}
					return
				}
				if status != "succeeded" || processed != 1 {
					t.Fatalf("finite zero cap must publish auto-success: status=%s processed=%d", status, processed)
				}
				var reason string
				var count int
				if err := db.QueryRow(`SELECT COUNT(*), MIN(reason_code) FROM events
					WHERE attempt_id = 'run_cap:custom_gate:1' AND event_type = 'attempt.succeeded'`).Scan(&count, &reason); err != nil {
					t.Fatal(err)
				}
				if count != 1 || reason != gate.reason {
					t.Fatalf("auto-success must retain its canonical reason and identity: count=%d reason=%q", count, reason)
				}
			})
		}
	}
}
