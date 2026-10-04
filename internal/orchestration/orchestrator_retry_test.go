package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/retryengine"
	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providerlimits"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

type staleRetryChainPlanner struct{}

func (staleRetryChainPlanner) Plan(_ errorcatalog.Code, retriesAttempted int) (retryengine.PlanResult, error) {
	return retryengine.PlanResult{
		ShouldRetry:      retriesAttempted < 2,
		MaxRetries:       2,
		NextDelay:        0,
		RetryClass:       "conditional_limited",
		EscalationTarget: "coordinator",
	}, nil
}

func TestClassifyExecutionReasonCodeUsesCanonicalTerminalCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "invalid validation response", err: &invalidValidationVerdictError{reason: "missing verdict"}, want: "llm_invalid_response"},
		{name: "invalid review response", err: &invalidExecutionReviewVerdictError{reason: "missing verdict"}, want: "llm_invalid_response"},
		{name: "pre-observation timeout", err: &executionNotStartedError{AttemptID: "attempt_1", NodeID: "node_1"}, want: "timeout"},
		{name: "setup handoff failure", err: errors.New("provider terminal handoff failed"), want: "internal_error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyExecutionReasonCode(tt.err); got != tt.want {
				t.Fatalf("reason code=%q, want %q", got, tt.want)
			}
		})
	}
}

func TestAutomaticWorkOrderRetryIsBlockedAfterMeaningfulProviderProgress(t *testing.T) {
	t.Parallel()
	db := openMigratedTestDB(t)
	policy, err := retryengine.LoadPolicyFromFile(filepath.Join(repoRoot(t), "policies", "retry_policy.yaml"), filepath.Join(repoRoot(t), "contracts", "schemas", "retry_policy.schema.json"))
	if err != nil {
		t.Fatalf("load retry policy: %v", err)
	}
	engine, err := retryengine.NewEngine(policy)
	if err != nil {
		t.Fatalf("create retry engine: %v", err)
	}
	loop := &orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil), retryEngine: engine}
	now := time.Date(2026, 3, 1, 12, 0, 0, 123456789, time.UTC)
	decision, planned, err := loop.planAutomaticWorkOrderRetry(context.Background(), asyncExecutionResult{
		run:                       runSnapshot{RunID: "run_1"},
		node:                      nodeDefinition{NodeID: "work_intake"},
		attempt:                   nodeAttempt{NodeRunID: "node_1"},
		attemptID:                 "run_1:work_intake:1",
		providerStreamObservation: opencode.InvocationStreamObservation{MeaningfulProgress: true},
	}, "timeout", now.Format(time.RFC3339))
	if err != nil {
		t.Fatalf("plan automatic retry: %v", err)
	}
	if !planned || decision.Decision != state.RetryDecisionFailFast || !decision.MeaningfulProgress {
		t.Fatalf("meaningful progress retry decision = %#v, planned=%t", decision, planned)
	}
}

func TestAutomaticWorkOrderRetryMeaningfulProgressDoesNotEscalateAtExhaustion(t *testing.T) {
	t.Parallel()
	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at) VALUES ('nr_prior_retry', 'run_1', 'work_intake', 2, 2, 'ready', 'governance', 'agent_governance', 1, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('prior_attempt', 'run_1', 'nr_1', 'committed_failure', 'provider_backed', 'server_response', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO retry_decisions (retry_decision_id, subject_kind, run_id, node_id, node_run_id, source_attempt_id, target_node_run_id, policy_id, retry_class, error_code, operator_text, meaningful_progress, decision, retry_index, max_retries, policy_delay_ns, due_at, dispatch_state, escalation_target, resolution, created_at) VALUES ('retry_prior_attempt', 'work_order_attempt', 'run_1', 'work_intake', 'nr_1', 'prior_attempt', 'nr_prior_retry', 'retry-policy-mvp-v1', 'internal_runtime', 'internal_error', 'retry', 0, 'retry', 1, 1, 0, '2026-03-01T12:00:00Z', 'pending', 'coordinator', 'pending', '2026-03-01T12:00:00Z')`)
	policy, err := retryengine.LoadPolicyFromFile(filepath.Join(repoRoot(t), "policies", "retry_policy.yaml"), filepath.Join(repoRoot(t), "contracts", "schemas", "retry_policy.schema.json"))
	if err != nil {
		t.Fatalf("load retry policy: %v", err)
	}
	engine, err := retryengine.NewEngine(policy)
	if err != nil {
		t.Fatalf("create retry engine: %v", err)
	}
	loop := &orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil), retryEngine: engine}
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	decision, planned, err := loop.planAutomaticWorkOrderRetry(context.Background(), asyncExecutionResult{
		run:                       runSnapshot{RunID: "run_1"},
		node:                      nodeDefinition{NodeID: "work_intake"},
		attempt:                   nodeAttempt{NodeRunID: "nr_1"},
		attemptID:                 "current_attempt",
		providerStreamObservation: opencode.InvocationStreamObservation{MeaningfulProgress: true},
	}, "internal_error", now.Format(time.RFC3339))
	if err != nil {
		t.Fatalf("plan exhausted automatic retry: %v", err)
	}
	if !planned || decision.Decision != state.RetryDecisionFailFast {
		t.Fatalf("meaningful exhausted retry decision = %#v, planned=%t", decision, planned)
	}
}

func TestProcessRunDispatchesDueRetryInSameTick(t *testing.T) {
	t.Parallel()
	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('source_attempt', 'run_1', 'nr_1', 'committed_failure', 'provider_backed', 'server_response', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	transition := state.NewTransitionService(db, nil)
	queued, scheduled, err := transition.RecordAutomaticWorkOrderRetry(context.Background(), state.AutomaticWorkOrderRetryRecordRequest{
		Transition: state.TransitionRequest{TransitionID: "tr_retry_source", EventID: "evt_retry_source", RunID: "run_1", NodeRunID: sql.NullString{String: "nr_1", Valid: true}, Entity: state.EntityTypeNode, FromState: "running", ToState: "failed", ActorID: "runtime", RoleID: "governance", AuthoritySource: "runtime", ReasonCode: "timeout", ReasonText: "timeout", CorrelationID: "corr", CausationID: "caus", OccurredAt: now.Format(time.RFC3339Nano), AttemptID: "source_attempt", SchemaVersion: "v1", EventType: "attempt.failed", EventSeq: 2, PayloadJSON: `{}`},
		Decision:   state.RetryDecisionCreateRequest{DecisionID: "retry_source", SubjectKind: state.RetrySubjectKindWorkOrderAttempt, RunID: "run_1", NodeID: "work_intake", NodeRunID: "nr_1", SourceAttemptID: "source_attempt", PolicyID: "policy", RetryClass: "transient_transport_provider_session", ErrorCode: "timeout", OperatorText: "retry", Decision: state.RetryDecisionRetry, RetryIndex: 1, MaxRetries: 3, PolicyDelay: 0, DueAt: now, CreatedAt: now, EscalationTarget: "coordinator"},
	})
	if err != nil || !scheduled {
		t.Fatalf("schedule retry: queued=%#v scheduled=%t err=%v", queued, scheduled, err)
	}
	loop := &orchestrationLoop{
		db:             db,
		transition:     transition,
		registry:       state.NewAgentRegistry(db),
		scheduler:      workflow.NewScheduler(workflow.SchedulerConfig{}),
		executionStore: executionstore.New(db),
		now:            func() time.Time { return now },
	}
	if err := loop.processRun(context.Background(), runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "approved", WorkflowDefinitionID: "wfdef_1"}); err != nil {
		t.Fatalf("process due retry: %v", err)
	}
	var attempts int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_executions WHERE node_run_id = ?`, queued.NodeRunID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("released retry was not dispatched in the same tick, attempt executions=%d", attempts)
	}
	var dispatchedAt string
	if err := db.QueryRow(`SELECT dispatched_at FROM retry_decisions WHERE retry_decision_id = 'retry_source'`).Scan(&dispatchedAt); err != nil {
		t.Fatal(err)
	}
	if dispatchedAt != now.Format(time.RFC3339Nano) {
		t.Fatalf("retry dispatched_at=%s, want %s", dispatchedAt, now.Format(time.RFC3339Nano))
	}
}

func TestAutomaticWorkOrderRetryUsesExactFailureTimeForProviderHint(t *testing.T) {
	t.Parallel()
	db := openMigratedTestDB(t)
	policy, err := retryengine.LoadPolicyFromFile(filepath.Join(repoRoot(t), "policies", "retry_policy.yaml"), filepath.Join(repoRoot(t), "contracts", "schemas", "retry_policy.schema.json"))
	if err != nil {
		t.Fatalf("load retry policy: %v", err)
	}
	engine, err := retryengine.NewEngine(policy)
	if err != nil {
		t.Fatalf("create retry engine: %v", err)
	}
	now := time.Date(2026, 3, 1, 12, 0, 0, 123456789, time.UTC)
	loop := &orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil), retryEngine: engine}
	decision, planned, err := loop.planAutomaticWorkOrderRetry(context.Background(), asyncExecutionResult{
		run:                       runSnapshot{RunID: "run_1"},
		node:                      nodeDefinition{NodeID: "work_intake"},
		attempt:                   nodeAttempt{NodeRunID: "node_1"},
		attemptID:                 "run_1:work_intake:1",
		providerDiagnosticsJSON:   `{"retry_after_ms":10000}`,
		providerStreamObservation: opencode.InvocationStreamObservation{},
	}, "timeout", now.Format(time.RFC3339Nano))
	if err != nil || !planned {
		t.Fatalf("plan automatic retry: planned=%t err=%v", planned, err)
	}
	want := now.Add(10 * time.Second)
	if !decision.DueAt.Equal(want) || decision.DueAt.Format(time.RFC3339Nano) != want.Format(time.RFC3339Nano) {
		t.Fatalf("retry due_at=%s, want %s", decision.DueAt.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

func TestOpenCodeRetryHintReachesPersistedAutomaticRetrySchedule(t *testing.T) {
	t.Parallel()
	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)
	now := time.Date(2026, 3, 1, 12, 0, 0, 123456789, time.UTC)
	const retryAfterMS = 10000
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('run_1:work_intake:1', 'run_1', 'nr_1', 'committed_failure', 'provider_backed', 'server_response', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

	binaryPath := filepath.Join(t.TempDir(), "retry-hint-opencode.sh")
	if err := os.WriteFile(binaryPath, []byte("#!/bin/sh\ncat >/dev/null || exit 1\nprintf '%s\\n' '{\"type\":\"error\",\"error\":{\"data\":{\"message\":\"rate limited\",\"statusCode\":429,\"retry_after_ms\":10000}}}'\nexit 17\n"), 0o700); err != nil {
		t.Fatalf("write fake opencode binary: %v", err)
	}
	adapter, err := opencode.NewSessionAdapter(opencode.SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}
	t.Cleanup(func() {
		if err := adapter.Close(); err != nil {
			t.Errorf("close retry-hint session adapter: %v", err)
		}
	})
	// Exceed pipe capacity so the fixture must consume the complete prompt before
	// it emits its provider error and exits.
	prompt := strings.Repeat("test", 64*1024)
	response, executeErr := adapter.Execute(context.Background(), opencode.ExecuteRequest{CompletionMode: opencode.CompletionModeServerResponse, SessionID: "session_retry_hint", AttemptID: "run_1:work_intake:1", RunID: "run_1", NodeRunID: "nr_1", WorkspacePath: t.TempDir(), Prompt: prompt, OccurredAt: now.Format(time.RFC3339Nano)})
	if executeErr == nil {
		t.Fatal("expected fake OpenCode provider failure")
	}

	policy, err := retryengine.LoadPolicyFromFile(filepath.Join(repoRoot(t), "policies", "retry_policy.yaml"), filepath.Join(repoRoot(t), "contracts", "schemas", "retry_policy.schema.json"))
	if err != nil {
		t.Fatalf("load retry policy: %v", err)
	}
	engine, err := retryengine.NewEngine(policy)
	if err != nil {
		t.Fatalf("create retry engine: %v", err)
	}
	loop := &orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil), retryEngine: engine}
	if response.Outcome.ReasonCode != "llm_rate_limited" {
		t.Fatalf("adapter outcome code=%q, want llm_rate_limited; execute error=%v; failure=%q; diagnostics=%s", response.Outcome.ReasonCode, executeErr, response.Outcome.FailureText, response.ProviderDiagnosticsJSON)
	}
	retry, planned, err := loop.planAutomaticWorkOrderRetry(context.Background(), asyncExecutionResult{run: runSnapshot{RunID: "run_1"}, node: nodeDefinition{NodeID: "work_intake"}, attempt: nodeAttempt{NodeRunID: "nr_1"}, attemptID: "run_1:work_intake:1", providerDiagnosticsJSON: response.ProviderDiagnosticsJSON, providerStreamObservation: response.StreamObservation}, response.Outcome.ReasonCode, now.Format(time.RFC3339Nano))
	if err != nil || !planned || retry.Decision != state.RetryDecisionRetry {
		t.Fatalf("plan retry from OpenCode result: retry=%#v planned=%t err=%v", retry, planned, err)
	}
	if _, scheduled, err := loop.transition.RecordAutomaticWorkOrderRetry(context.Background(), state.AutomaticWorkOrderRetryRecordRequest{Transition: state.TransitionRequest{TransitionID: "tr_opencode_retry_hint", EventID: "evt_opencode_retry_hint", RunID: "run_1", NodeRunID: sql.NullString{String: "nr_1", Valid: true}, Entity: state.EntityTypeNode, FromState: "running", ToState: "failed", ActorID: "runtime_orchestrator", RoleID: "governance", AuthoritySource: "runtime.orchestrator", ReasonCode: "llm_rate_limited", ReasonText: "rate limited", CorrelationID: "corr_opencode_retry_hint", CausationID: "caus_opencode_retry_hint", OccurredAt: now.Format(time.RFC3339Nano), AttemptID: "run_1:work_intake:1", SchemaVersion: "v1", EventType: "attempt.failed", EventSeq: 1, PayloadJSON: `{}`}, Decision: retry}); err != nil || !scheduled {
		t.Fatalf("persist retry from OpenCode result: scheduled=%t err=%v", scheduled, err)
	}
	var providerRetryAt, dueAt string
	if err := db.QueryRow(`SELECT provider_retry_at, due_at FROM retry_decisions WHERE retry_decision_id = ?`, retry.DecisionID).Scan(&providerRetryAt, &dueAt); err != nil {
		t.Fatalf("load persisted retry schedule: %v", err)
	}
	want := now.Add(retryAfterMS * time.Millisecond).Format(time.RFC3339Nano)
	if providerRetryAt != want || dueAt != want {
		t.Fatalf("persisted OpenCode retry hint provider_retry_at=%q due_at=%q, want %q", providerRetryAt, dueAt, want)
	}
}

func TestAutomaticRetryKeepsRunRunningUntilSuccessfulRetryFinalizes(t *testing.T) {
	t.Parallel()
	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)
	const sourceAttemptID = "run_1:work_intake:1"
	now := time.Date(2026, 3, 19, 18, 30, 0, 0, time.UTC)
	dueAt := now.Add(time.Minute)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('`+sourceAttemptID+`', 'run_1', 'nr_1', 'committed_failure', 'provider_backed', 'server_response', '`+now.Format(time.RFC3339Nano)+`', '`+now.Format(time.RFC3339Nano)+`')`)

	transition := state.NewTransitionService(db, nil)
	queued, scheduled, err := transition.RecordAutomaticWorkOrderRetry(context.Background(), state.AutomaticWorkOrderRetryRecordRequest{
		Transition: state.TransitionRequest{TransitionID: "tr_retry_source_failed", EventID: "evt_retry_source_failed", RunID: "run_1", NodeRunID: sql.NullString{String: "nr_1", Valid: true}, Entity: state.EntityTypeNode, FromState: "running", ToState: "failed", ActorID: "runtime_orchestrator", RoleID: "governance", AuthoritySource: "runtime.orchestrator", ReasonCode: "timeout", ReasonText: "provider timed out", CorrelationID: "corr_retry_lifecycle", CausationID: "caus_retry_lifecycle_source", OccurredAt: now.Format(time.RFC3339Nano), AttemptID: sourceAttemptID, SchemaVersion: "v1", EventType: "attempt.failed", EventSeq: 2, PayloadJSON: `{}`},
		Decision:   state.RetryDecisionCreateRequest{DecisionID: "retry_source_lifecycle", SubjectKind: state.RetrySubjectKindWorkOrderAttempt, RunID: "run_1", NodeID: "work_intake", NodeRunID: "nr_1", SourceAttemptID: sourceAttemptID, PolicyID: "retry-policy", RetryClass: "transient_transport_provider_session", ErrorCode: "timeout", OperatorText: "retry after backoff", Decision: state.RetryDecisionRetry, RetryIndex: 1, MaxRetries: 1, PolicyDelay: time.Minute, DueAt: dueAt, CreatedAt: now, EscalationTarget: "coordinator"},
	})
	if err != nil || !scheduled {
		t.Fatalf("schedule automatic retry: queued=%#v scheduled=%t err=%v", queued, scheduled, err)
	}

	loop := &orchestrationLoop{db: db, transition: transition, now: func() time.Time { return now }}
	nodes := []nodeDefinition{{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"}}
	if err := loop.finalizeRunState(context.Background(), runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "draft", AutonomousApprovals: true}, nodes, nil); err != nil {
		t.Fatalf("finalize before retry due: %v", err)
	}
	if status := queryRunStatus(t, db, "run_1"); status != "running" {
		t.Fatalf("run status before retry due=%q, want running", status)
	}

	if released, err := transition.ReleaseDueWorkOrderRetries(context.Background(), "run_1", dueAt.Format(time.RFC3339Nano)); err != nil || len(released) != 1 || released[0].NodeRunID != queued.NodeRunID {
		t.Fatalf("release due retry: released=%#v err=%v", released, err)
	}
	const retryAttemptID = "run_1:work_intake:2"
	if err := transition.Apply(context.Background(), state.TransitionRequest{TransitionID: "tr_retry_target_running", EventID: "evt_retry_target_running", RunID: "run_1", NodeRunID: sql.NullString{String: queued.NodeRunID, Valid: true}, Entity: state.EntityTypeNode, FromState: "ready", ToState: "running", ActorID: "runtime_orchestrator", RoleID: "governance", AuthoritySource: "runtime.orchestrator", ReasonCode: "orchestration_progress", ReasonText: "retry dispatched", CorrelationID: "corr_retry_lifecycle", CausationID: "caus_retry_lifecycle_dispatch", OccurredAt: dueAt.Format(time.RFC3339Nano), AttemptID: retryAttemptID, SchemaVersion: "v1", EventType: "attempt.started", EventSeq: 2, PayloadJSON: `{}`}); err != nil {
		t.Fatalf("start retry target: %v", err)
	}
	if err := transition.RecordNodeCompletion(context.Background(), state.NodeCompletionRecordRequest{Transition: state.TransitionRequest{TransitionID: "tr_retry_target_succeeded", EventID: "evt_retry_target_succeeded", RunID: "run_1", NodeRunID: sql.NullString{String: queued.NodeRunID, Valid: true}, Entity: state.EntityTypeNode, FromState: "running", ToState: "succeeded", ActorID: "runtime_orchestrator", RoleID: "governance", AuthoritySource: "runtime.orchestrator", ReasonCode: "execution_completed", ReasonText: "retry succeeded", CorrelationID: "corr_retry_lifecycle", CausationID: "caus_retry_lifecycle_success", OccurredAt: dueAt.Add(time.Second).Format(time.RFC3339Nano), AttemptID: retryAttemptID, SchemaVersion: "v1", EventType: "attempt.succeeded", EventSeq: 3, PayloadJSON: `{}`}}); err != nil {
		t.Fatalf("complete retry target: %v", err)
	}
	loop.now = func() time.Time { return dueAt.Add(time.Second) }
	if err := loop.finalizeRunState(context.Background(), runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "draft", AutonomousApprovals: true}, nodes, nil); err != nil {
		t.Fatalf("finalize successful retry: %v", err)
	}
	if status := queryRunStatus(t, db, "run_1"); status != "succeeded" {
		t.Fatalf("run status after successful retry=%q, want succeeded", status)
	}
	var dispatchState, resolution string
	if err := db.QueryRow(`SELECT dispatch_state, resolution FROM retry_decisions WHERE retry_decision_id = 'retry_source_lifecycle'`).Scan(&dispatchState, &resolution); err != nil {
		t.Fatal(err)
	}
	if dispatchState != string(state.RetryDispatchStateDispatched) || resolution != string(state.RetryDecisionResolutionRecovered) {
		t.Fatalf("retry decision dispatch=%q resolution=%q, want dispatched/recovered", dispatchState, resolution)
	}
}

func TestAutomaticRetryFinalizationRequiresCoverageForEachFailedNode(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)
	const now = "2026-03-19T18:30:00Z"
	mustExec(t, db, `UPDATE node_runs SET node_status = 'failed', finished_at = '`+now+`' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_parallel_failed', 'run_1', 'parallel_node', 1, 1, 'failed', 'governance', 'agent_governance', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version) VALUES ('nr_retry_intake', 'run_1', 'work_intake', 2, 2, 'pending', 'governance', 'agent_governance', 1)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version) VALUES ('nr_retry_parallel', 'run_1', 'parallel_node', 2, 2, 'pending', 'governance', 'agent_governance', 1)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('attempt_intake_failed', 'run_1', 'nr_1', 'committed_failure', 'provider_backed', 'server_response', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('attempt_parallel_failed', 'run_1', 'nr_parallel_failed', 'committed_failure', 'provider_backed', 'server_response', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO retry_decisions (retry_decision_id, subject_kind, run_id, node_id, node_run_id, source_attempt_id, target_node_run_id, policy_id, retry_class, error_code, operator_text, meaningful_progress, decision, retry_index, max_retries, policy_delay_ns, due_at, dispatch_state, escalation_target, resolution, created_at) VALUES ('retry_intake', 'work_order_attempt', 'run_1', 'work_intake', 'nr_1', 'attempt_intake_failed', 'nr_retry_intake', 'retry-policy', 'transient_transport_provider_session', 'timeout', 'retry', 0, 'retry', 1, 1, 0, '`+now+`', 'pending', 'coordinator', 'pending', '`+now+`')`)

	loop := &orchestrationLoop{db: db}
	covered, err := loop.hasOutstandingAutomaticWorkOrderRetriesForFailedNodes(context.Background(), "run_1", []string{"work_intake", "parallel_node"})
	if err != nil {
		t.Fatalf("check incomplete retry coverage: %v", err)
	}
	if covered {
		t.Fatal("an unrelated retry must not cover a fail-fast parallel node")
	}

	mustExec(t, db, `INSERT INTO retry_decisions (retry_decision_id, subject_kind, run_id, node_id, node_run_id, source_attempt_id, target_node_run_id, policy_id, retry_class, error_code, operator_text, meaningful_progress, decision, retry_index, max_retries, policy_delay_ns, due_at, dispatch_state, escalation_target, resolution, created_at) VALUES ('retry_parallel', 'work_order_attempt', 'run_1', 'parallel_node', 'nr_parallel_failed', 'attempt_parallel_failed', 'nr_retry_parallel', 'retry-policy', 'transient_transport_provider_session', 'timeout', 'retry', 0, 'retry', 1, 1, 0, '`+now+`', 'pending', 'coordinator', 'pending', '`+now+`')`)
	covered, err = loop.hasOutstandingAutomaticWorkOrderRetriesForFailedNodes(context.Background(), "run_1", []string{"work_intake", "parallel_node"})
	if err != nil {
		t.Fatalf("check complete retry coverage: %v", err)
	}
	if !covered {
		t.Fatal("every failed parallel node with an unresolved retry must defer finalization")
	}
}

func TestStaleDispatchedRetryFailureChainTerminalizesRun(t *testing.T) {
	t.Parallel()
	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)
	ctx := context.Background()
	now := time.Date(2026, 3, 19, 18, 30, 0, 0, time.UTC)
	transition := state.NewTransitionService(db, nil)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '2026-03-19T18:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'idle', NULL, 0, '2026-03-19T18:00:00Z', '2026-03-19T18:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance_backup', 'governance', 'idle', NULL, 0, '2026-03-19T18:00:00Z', '2026-03-19T18:00:00Z')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('source_attempt', 'run_1', 'nr_1', 'committed_failure', 'provider_backed', 'server_response', '2026-03-19T18:30:00Z', '2026-03-19T18:30:00Z')`)

	first, scheduled, err := transition.RecordAutomaticWorkOrderRetry(ctx, state.AutomaticWorkOrderRetryRecordRequest{
		Transition: state.TransitionRequest{TransitionID: "tr_source_failed", EventID: "evt_source_failed", RunID: "run_1", NodeRunID: sql.NullString{String: "nr_1", Valid: true}, Entity: state.EntityTypeNode, FromState: "running", ToState: "failed", ActorID: "runtime_orchestrator", RoleID: "governance", AuthoritySource: "runtime.orchestrator", ReasonCode: "timeout", ReasonText: "provider failed", CorrelationID: "corr_stale_retry_chain", CausationID: "caus_source_failed", OccurredAt: now.Format(time.RFC3339Nano), AttemptID: "source_attempt", SchemaVersion: "v1", EventType: "attempt.failed", EventSeq: 2, PayloadJSON: `{}`},
		Decision:   state.RetryDecisionCreateRequest{DecisionID: "retry_source_attempt", SubjectKind: state.RetrySubjectKindWorkOrderAttempt, RunID: "run_1", NodeID: "work_intake", NodeRunID: "nr_1", SourceAttemptID: "source_attempt", PolicyID: "retry-policy", RetryClass: "transient_transport_provider_session", ErrorCode: "timeout", OperatorText: "retry", Decision: state.RetryDecisionRetry, RetryIndex: 1, MaxRetries: 2, DueAt: now, CreatedAt: now, EscalationTarget: "coordinator"},
	})
	if err != nil || !scheduled {
		t.Fatalf("schedule first retry: queued=%#v scheduled=%t err=%v", first, scheduled, err)
	}
	if _, err := transition.ReleaseDueWorkOrderRetries(ctx, "run_1", now.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("release first retry: %v", err)
	}
	firstAttemptID := "run_1:work_intake:2"
	if err := transition.Apply(ctx, staleRetryTargetStartTransition(first.NodeRunID, firstAttemptID, now.Add(time.Second))); err != nil {
		t.Fatalf("start first retry target: %v", err)
	}

	reconciler := state.NewReconciler(db, state.NewSessionManager(db, state.NewSessionLifecycleMetrics()), staleRetryChainPlanner{}, nil)
	reconciledAt := now.Add(10 * time.Minute)
	if result, err := reconciler.Reconcile(ctx, state.ReconcileRequest{Now: reconciledAt.Format(time.RFC3339Nano)}); err != nil || result.AttemptsReassigned != 1 {
		t.Fatalf("fence stale retry target: result=%+v err=%v", result, err)
	}
	var firstResolution string
	if err := db.QueryRow(`SELECT resolution FROM retry_decisions WHERE retry_decision_id = 'retry_source_attempt'`).Scan(&firstResolution); err != nil {
		t.Fatal(err)
	}
	if firstResolution != string(state.RetryDecisionResolutionFailed) {
		t.Fatalf("stale target predecessor resolution=%q, want failed", firstResolution)
	}

	var successor string
	if err := db.QueryRow(`SELECT target_node_run_id FROM retry_decisions WHERE source_attempt_id = ?`, firstAttemptID).Scan(&successor); err != nil {
		t.Fatalf("load stale target successor: %v", err)
	}
	if _, err := transition.ReleaseDueWorkOrderRetries(ctx, "run_1", reconciledAt.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("release stale target successor: %v", err)
	}
	successorAttemptID := "run_1:work_intake:3"
	if err := transition.Apply(ctx, staleRetryTargetStartTransition(successor, successorAttemptID, reconciledAt.Add(time.Second))); err != nil {
		t.Fatalf("start stale target successor: %v", err)
	}
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('`+successorAttemptID+`', 'run_1', '`+successor+`', 'committed_failure', 'provider_backed', 'server_response', '`+reconciledAt.Format(time.RFC3339Nano)+`', '`+reconciledAt.Format(time.RFC3339Nano)+`')`)
	if _, scheduled, err := transition.RecordAutomaticWorkOrderRetry(ctx, state.AutomaticWorkOrderRetryRecordRequest{
		Transition: state.TransitionRequest{TransitionID: "tr_successor_failed", EventID: "evt_successor_failed", RunID: "run_1", NodeRunID: sql.NullString{String: successor, Valid: true}, Entity: state.EntityTypeNode, FromState: "running", ToState: "failed", ActorID: "runtime_orchestrator", RoleID: "governance", AuthoritySource: "runtime.orchestrator", ReasonCode: "provider_unknown_failure", ReasonText: "terminal failure", CorrelationID: "corr_stale_retry_chain", CausationID: "caus_successor_failed", OccurredAt: reconciledAt.Add(2 * time.Second).Format(time.RFC3339Nano), AttemptID: successorAttemptID, SchemaVersion: "v1", EventType: "attempt.failed", EventSeq: 3, PayloadJSON: `{}`},
		Decision:   state.RetryDecisionCreateRequest{DecisionID: "retry_successor_attempt", SubjectKind: state.RetrySubjectKindWorkOrderAttempt, RunID: "run_1", NodeID: "work_intake", NodeRunID: successor, SourceAttemptID: successorAttemptID, PolicyID: "retry-policy", RetryClass: "non_retryable", ErrorCode: "provider_unknown_failure", OperatorText: "fail fast", Decision: state.RetryDecisionFailFast, RetryIndex: 2, MaxRetries: 2, DueAt: reconciledAt.Add(2 * time.Second), CreatedAt: reconciledAt.Add(2 * time.Second), EscalationTarget: "coordinator"},
	}); err != nil || scheduled {
		t.Fatalf("record terminal successor failure: scheduled=%t err=%v", scheduled, err)
	}

	loop := &orchestrationLoop{db: db, transition: transition, now: func() time.Time { return reconciledAt.Add(2 * time.Second) }}
	nodes := []nodeDefinition{{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"}}
	if err := loop.finalizeRunState(ctx, runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "draft", AutonomousApprovals: true}, nodes, nil); err != nil {
		t.Fatalf("finalize terminal retry chain: %v", err)
	}
	if status := queryRunStatus(t, db, "run_1"); status != "failed" {
		t.Fatalf("run status after terminal retry chain=%q, want failed", status)
	}
}

func staleRetryTargetStartTransition(nodeRunID, attemptID string, occurredAt time.Time) state.TransitionRequest {
	return state.TransitionRequest{
		TransitionID:    "tr_start_" + nodeRunID,
		EventID:         "evt_start_" + nodeRunID,
		RunID:           "run_1",
		NodeRunID:       sql.NullString{String: nodeRunID, Valid: true},
		Entity:          state.EntityTypeNode,
		FromState:       "ready",
		ToState:         "running",
		ActorID:         "runtime_orchestrator",
		RoleID:          "governance",
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "orchestration_progress",
		ReasonText:      "retry dispatched",
		CorrelationID:   "corr_stale_retry_chain",
		CausationID:     "caus_start_" + nodeRunID,
		OccurredAt:      occurredAt.UTC().Format(time.RFC3339Nano),
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.started",
		EventSeq:        2,
		PayloadJSON:     `{}`,
	}
}

func TestAdapterTerminalFailureReasonIsConsistentAcrossSettlementProjectionAndRetry(t *testing.T) {
	t.Parallel()

	for _, legacyCode := range []string{"provider_process_failed", "provider_malformed_protocol", "provider_terminal_failure"} {
		t.Run(legacyCode, func(t *testing.T) {
			db := openMigratedTestDB(t)
			seedMinimumGraph(t, db)
			const attemptID = "run_1:work_intake:1"
			const invocationID = "pinv_terminal_failure"
			const sessionID = "session_terminal_failure"
			const providerSessionID = "ses_terminal_failure"
			const now = "2026-03-19T18:30:00Z"

			store := executionstore.New(db)
			if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, SessionRef: sessionID, CreatedAt: now}); err != nil {
				t.Fatalf("enqueue attempt: %v", err)
			}
			claim, acquired, err := store.ClaimQueuedProviderAttempt(context.Background(), executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "terminal-failure-worker", LeaseUntil: "2099-03-01T12:00:00Z", ClaimedAt: now})
			if err != nil || !acquired {
				t.Fatalf("claim attempt: acquired=%t err=%v", acquired, err)
			}
			mustExec(t, db, `UPDATE attempt_executions SET state = 'starting', launch_started_at = '`+now+`' WHERE attempt_id = '`+attemptID+`'`)
			seedOpenProviderInvocationForAttempt(t, store, attemptID, "run_1", "nr_1", sessionID, providerSessionID, invocationID, now)
			attachQuiescentProviderProcessGuard(t, store, invocationID, attemptID, "run_1", "nr_1", claim.LeaseOwner, claim.LeaseVersion, now)

			policy, err := retryengine.LoadPolicyFromFile(filepath.Join(repoRoot(t), "policies", "retry_policy.yaml"), filepath.Join(repoRoot(t), "contracts", "schemas", "retry_policy.schema.json"))
			if err != nil {
				t.Fatalf("load retry policy: %v", err)
			}
			retries, err := retryengine.NewEngine(policy)
			if err != nil {
				t.Fatalf("create retry engine: %v", err)
			}
			nowTime, err := time.Parse(time.RFC3339, now)
			if err != nil {
				t.Fatal(err)
			}
			loop := &orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil), executionStore: store, retryEngine: retries, now: func() time.Time { return nowTime }}
			job := asyncExecutionResult{
				run:                       runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "draft"},
				node:                      nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
				attempt:                   nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, RequiredRole: "governance", AgentID: "agent_governance", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse},
				attemptID:                 attemptID,
				sessionID:                 sessionID,
				providerSessionID:         providerSessionID,
				providerInvocationID:      invocationID,
				providerInvocationAt:      now,
				completionMode:            executionstore.ProviderCompletionModeServerResponse,
				expectedLeaseOwner:        claim.LeaseOwner,
				expectedLeaseVersion:      claim.LeaseVersion,
				occurredAt:                now,
				outcome:                   opencode.ExecutionOutcome{State: "failed", ReasonCode: legacyCode, FailureText: "provider terminal failure", EvidenceSource: "stream"},
				providerStreamObservation: opencode.InvocationStreamObservation{ProviderSessionID: providerSessionID},
			}
			persistProviderTerminalHandoffForTest(t, loop, &job)
			if err := loop.commitExecutionCompletion(context.Background(), job); err != nil {
				t.Fatalf("settle provider failure: %v", err)
			}
			if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
				t.Fatalf("project provider failure: %v", err)
			}

			var failureCode, eventReasonCode, retryCode, retryDecision string
			if err := db.QueryRow(`SELECT failure_code FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&failureCode); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT reason_code FROM events WHERE attempt_id = ? AND event_type = 'attempt.failed'`, attemptID).Scan(&eventReasonCode); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT error_code, decision FROM retry_decisions WHERE source_attempt_id = ?`, attemptID).Scan(&retryCode, &retryDecision); err != nil {
				t.Fatal(err)
			}
			if failureCode != "provider_unknown_failure" || eventReasonCode != failureCode || retryCode != failureCode || retryDecision != string(state.RetryDecisionFailFast) {
				t.Fatalf("terminal reason lineage failure=%q event=%q retry=%q decision=%q", failureCode, eventReasonCode, retryCode, retryDecision)
			}
		})
	}
}

func TestResourceLimitFailureSettlesDurablyWithoutRetry(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)
	const attemptID = "run_1:work_intake:1"
	const invocationID = "pinv_resource_limit"
	const sessionID = "session_resource_limit"
	const providerSessionID = "ses_resource_limit"
	const now = "2026-03-19T18:30:00Z"

	store := executionstore.New(db)
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, SessionRef: sessionID, CreatedAt: now}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	claim, acquired, err := store.ClaimQueuedProviderAttempt(context.Background(), executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "resource-limit-worker", LeaseUntil: "2099-03-01T12:00:00Z", ClaimedAt: now})
	if err != nil || !acquired {
		t.Fatalf("claim attempt: acquired=%t err=%v", acquired, err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state = 'starting', launch_started_at = '`+now+`' WHERE attempt_id = '`+attemptID+`'`)
	seedOpenProviderInvocationForAttempt(t, store, attemptID, "run_1", "nr_1", sessionID, providerSessionID, invocationID, now)
	attachQuiescentProviderProcessGuard(t, store, invocationID, attemptID, "run_1", "nr_1", claim.LeaseOwner, claim.LeaseVersion, now)

	policy, err := retryengine.LoadPolicyFromFile(filepath.Join(repoRoot(t), "policies", "retry_policy.yaml"), filepath.Join(repoRoot(t), "contracts", "schemas", "retry_policy.schema.json"))
	if err != nil {
		t.Fatalf("load retry policy: %v", err)
	}
	retries, err := retryengine.NewEngine(policy)
	if err != nil {
		t.Fatalf("create retry engine: %v", err)
	}
	nowTime, err := time.Parse(time.RFC3339, now)
	if err != nil {
		t.Fatal(err)
	}
	limitErr := &providerlimits.DepthExceededError{Resource: "snapshot lineage", Limit: providerlimits.SnapshotLineageDepth}
	outcome := normalizeExecutionOutcome(opencode.ExecutionOutcome{}, limitErr)
	if outcome.ReasonCode != "resource_limit_exceeded" || outcome.FailureText != limitErr.Error() {
		t.Fatalf("resource-limit execution outcome=%+v", outcome)
	}
	loop := &orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil), mailbox: state.NewMailboxStore(db, nil), events: state.NewAttemptEventIngestor(db, nil), executionStore: store, retryEngine: retries, now: func() time.Time { return nowTime }}
	job := asyncExecutionResult{
		run:                       runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "draft"},
		node:                      nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
		attempt:                   nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, RequiredRole: "governance", AgentID: "agent_governance", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse},
		attemptID:                 attemptID,
		sessionID:                 sessionID,
		providerSessionID:         providerSessionID,
		providerInvocationID:      invocationID,
		providerInvocationAt:      now,
		completionMode:            executionstore.ProviderCompletionModeServerResponse,
		expectedLeaseOwner:        claim.LeaseOwner,
		expectedLeaseVersion:      claim.LeaseVersion,
		occurredAt:                now,
		outcome:                   outcome,
		err:                       limitErr,
		providerStreamObservation: opencode.InvocationStreamObservation{ProviderSessionID: providerSessionID},
	}
	persistProviderTerminalHandoffForTest(t, loop, &job)
	if err := loop.commitExecutionCompletion(context.Background(), job); err != nil {
		t.Fatalf("settle resource-limit failure: %v", err)
	}
	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("project resource-limit failure: %v", err)
	}

	var failureCode, failureText, eventReasonCode, retryCode, retryDecision, responseText string
	if err := db.QueryRow(`SELECT failure_code, failure_text FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&failureCode, &failureText); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT reason_code FROM events WHERE attempt_id = ? AND event_type = 'attempt.failed'`, attemptID).Scan(&eventReasonCode); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT error_code, decision FROM retry_decisions WHERE source_attempt_id = ?`, attemptID).Scan(&retryCode, &retryDecision); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT json_extract(payload_json, '$.response_full') FROM mailbox_messages WHERE run_id = ? AND json_extract(payload_json, '$.communication_type') = 'response_received'`, "run_1").Scan(&responseText); err != nil {
		t.Fatal(err)
	}
	if failureCode != "resource_limit_exceeded" || eventReasonCode != failureCode || retryCode != failureCode || retryDecision != string(state.RetryDecisionFailFast) || failureText != limitErr.Error() || responseText != limitErr.Error() {
		t.Fatalf("resource-limit failure lineage code=%q event=%q retry=%q decision=%q text=%q response=%q", failureCode, eventReasonCode, retryCode, retryDecision, failureText, responseText)
	}
}
