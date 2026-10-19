package orchestration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"kingdom/internal/core/retryengine"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
)

func TestProviderFailureSchedulesRetryWhileLaunchObservationIsDelayed(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	const attemptID = "run_1:work_intake:1"
	const invocationID = "pinv_starting_failure"
	const sessionID = "session_starting_failure"
	const now = "2026-03-01T12:00:05.123456789Z"
	mustExec(t, db, `UPDATE node_runs SET node_status = 'starting', started_at = NULL WHERE node_run_id = 'nr_1'`)

	store := executionstore.New(db)
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, SessionRef: sessionID, CreatedAt: now}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	claim, acquired, err := store.ClaimQueuedProviderAttempt(context.Background(), executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "launch-observation-delayed", LeaseUntil: "2099-03-01T12:00:00Z", ClaimedAt: now})
	if err != nil || !acquired {
		t.Fatalf("claim attempt: acquired=%t err=%v", acquired, err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state = 'starting', launch_started_at = '2026-03-01T12:00:05.123456789Z' WHERE attempt_id = 'run_1:work_intake:1'`)
	requests := newRequestFixture(db, nil)
	seedOpenProviderInvocationForAttempt(t, store, requests, attemptID, "run_1", "nr_1", sessionID, invocationID, now)
	response := requests.run(t, attemptID, invocationID, opencode.ExecuteResponse{
		Outcome:           opencode.ExecutionOutcome{State: "failed", ReasonCode: "llm_rate_limited", FailureText: "provider rate limit", EvidenceSource: "stream"},
		StreamObservation: opencode.InvocationStreamObservation{ProviderErrorPresent: true, ProviderErrorStatus: 429},
	}, nil)

	policy, err := retryengine.LoadPolicyFromFile(filepath.Join(repoRoot(t), "policies", "retry_policy.yaml"), filepath.Join(repoRoot(t), "contracts", "schemas", "retry_policy.schema.json"))
	if err != nil {
		t.Fatalf("load retry policy: %v", err)
	}
	retries, err := retryengine.NewEngine(policy)
	if err != nil {
		t.Fatalf("create retry engine: %v", err)
	}
	nowTime, err := time.Parse(time.RFC3339Nano, now)
	if err != nil {
		t.Fatal(err)
	}
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	loop := &orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil), executionStore: store, fsStore: fsStore, retryEngine: retries, now: func() time.Time { return nowTime }}
	job := asyncExecutionResult{
		run:                       runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "approved"},
		node:                      nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
		attempt:                   nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, RequiredRole: "governance", AgentID: "agent_governance", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse},
		attemptID:                 attemptID,
		sessionID:                 sessionID,
		providerSessionID:         response.ProviderSessionID,
		providerInvocationID:      invocationID,
		providerInvocationAt:      now,
		completionMode:            executionstore.ProviderCompletionModeServerResponse,
		expectedLeaseOwner:        claim.LeaseOwner,
		expectedLeaseVersion:      claim.LeaseVersion,
		occurredAt:                now,
		outcome:                   response.Outcome,
		providerStreamObservation: response.StreamObservation,
	}
	if err := loop.persistProviderTerminalHandoff(context.Background(), &job); err != nil {
		t.Fatalf("persist provider terminal handoff: %v", err)
	}

	// The launch observer has not committed, but the provider's terminal result
	// is lease-fenced and can safely settle the still-starting attempt.
	if err := loop.commitExecutionCompletion(context.Background(), job); err != nil {
		t.Fatalf("commit provider failure before launch observation: %v", err)
	}
	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("project provider failure before launch observation: %v", err)
	}
	// Replay the delayed launch observer after the failure. It may record the
	// observed timestamp, but cannot invent a running transition or another retry.
	if err := loop.ensureExecutionLaunchPersisted(context.Background(), job, `{}`, now); err != nil {
		t.Fatalf("persist delayed launch observation after provider failure: %v", err)
	}

	var decisions, successors, launchEvents int
	if err := db.QueryRow(`SELECT COUNT(1) FROM retry_decisions WHERE source_attempt_id = ? AND decision = 'retry'`, attemptID).Scan(&decisions); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1' AND node_id = 'work_intake' AND attempt = 2`).Scan(&successors); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = ? AND event_type = 'attempt.started'`, attemptID).Scan(&launchEvents); err != nil {
		t.Fatal(err)
	}
	if decisions != 1 || successors != 1 || launchEvents != 0 {
		t.Fatalf("starting provider failure created decisions=%d successors=%d launch_events=%d, want one retry and successor with no fabricated launch event", decisions, successors, launchEvents)
	}
	var sourceState string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_1'`).Scan(&sourceState); err != nil {
		t.Fatal(err)
	}
	if sourceState != "failed" {
		t.Fatalf("starting source node state=%q, want failed", sourceState)
	}
}
