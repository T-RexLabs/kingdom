package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/providers/opencode"
)

type scriptedExecutionStep struct {
	output  string
	err     error
	block   <-chan struct{}
	started chan<- struct{}
}

type scriptedExecutionAdapter struct {
	mu       sync.Mutex
	steps    map[string]scriptedExecutionStep
	requests []opencode.ExecuteRequest
}

func newScriptedExecutionAdapter(steps map[string]scriptedExecutionStep) *scriptedExecutionAdapter {
	cloned := make(map[string]scriptedExecutionStep, len(steps))
	for attemptID, step := range steps {
		cloned[attemptID] = step
	}
	return &scriptedExecutionAdapter{steps: cloned}
}

func (a *scriptedExecutionAdapter) Execute(ctx context.Context, req opencode.ExecuteRequest) (response opencode.ExecuteResponse, executeErr error) {
	ownership, quiescence := testProcessGuardCallbacks(req)
	if req.OnProcessGuardOwnership != nil {
		if err := req.OnProcessGuardOwnership(ctx, ownership); err != nil {
			return opencode.ExecuteResponse{}, fmt.Errorf("acknowledge scripted process guard: %w", err)
		}
	}
	if req.OnProcessStarted != nil {
		req.OnProcessStarted()
	}
	defer func() {
		if req.OnProcessGuardQuiescence != nil {
			if err := req.OnProcessGuardQuiescence(context.WithoutCancel(ctx), quiescence); err != nil {
				executeErr = errors.Join(executeErr, fmt.Errorf("record scripted process guard quiescence: %w", err))
			}
		}
	}()
	a.mu.Lock()
	a.requests = append(a.requests, req)
	step, ok := a.steps[req.AttemptID]
	a.mu.Unlock()
	if !ok {
		output := `{"summary":"ok"}`
		return opencode.ExecuteResponse{Accepted: true, OutputText: output, ProviderInvocationID: req.InvocationID, ProviderInvocationAt: req.InvocationIssuedAt, StreamObservation: testSuccessfulExecuteObservation(req, output)}, nil
	}
	if step.started != nil {
		close(step.started)
	}
	if step.block != nil {
		select {
		case <-step.block:
		case <-ctx.Done():
			return opencode.ExecuteResponse{}, ctx.Err()
		}
	}
	if step.err != nil {
		return opencode.ExecuteResponse{}, step.err
	}
	response = opencode.ExecuteResponse{
		Accepted:             true,
		OutputText:           step.output,
		ProviderInvocationID: req.InvocationID,
		ProviderInvocationAt: req.InvocationIssuedAt,
		StreamObservation:    testSuccessfulExecuteObservation(req, step.output),
	}
	return response, nil
}

func (a *scriptedExecutionAdapter) Cancel(context.Context, opencode.CancelRequest) error {
	return nil
}

func (a *scriptedExecutionAdapter) Stream(context.Context, opencode.StreamRequest) (<-chan opencode.AttemptStreamEvent, <-chan error, error) {
	eventCh := make(chan opencode.AttemptStreamEvent)
	errCh := make(chan error)
	close(eventCh)
	close(errCh)
	return eventCh, errCh, nil
}

func TestRuntimeDispatchLoopValidationReworkWaitsForWaveSettlementAndSucceeds(t *testing.T) {
	db := openMigratedTestDB(t)
	seedValidationReworkFlowRun(t, db)

	releaseNightValidator := make(chan struct{})
	nightValidatorStarted := make(chan struct{})
	var releaseNightValidatorOnce sync.Once
	releaseNight := func() {
		releaseNightValidatorOnce.Do(func() {
			close(releaseNightValidator)
		})
	}
	adapter := newScriptedExecutionAdapter(map[string]scriptedExecutionStep{
		"run_rework_flow:execution_kickoff:1":   {output: "Kickoff confirmed."},
		"run_rework_flow:implementation:1":      {output: "Initial implementation complete."},
		"run_rework_flow:execution_review:1":    {output: "VERDICT: PASS\nInitial review looks good."},
		"run_rework_flow:quality_validation:1":  {output: "VERDICT: REWORK\nFix the validator blocker."},
		"run_rework_flow:security_validation:1": {output: "VERDICT: PASS\nNight watch checks are green.", block: releaseNightValidator, started: nightValidatorStarted},
		"run_rework_flow:implementation:2":      {output: "Implemented follow-up fixes."},
		"run_rework_flow:execution_review:2":    {output: "VERDICT: PASS\nFollow-up review is clean."},
		"run_rework_flow:quality_validation:2":  {output: "VERDICT: PASS\nQA checks are green."},
		"run_rework_flow:security_validation:2": {output: "VERDICT: PASS\nNight watch confirms the fix."},
		"run_rework_flow:completion:1":          {output: "Royal closure recorded."},
	})

	rt, err := NewRuntime(Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		Adapter:          adapter,
		GovernanceMode:   "autonomous",
		DispatchInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	ctx, cancel := testDeadlineContext(t)
	defer cancel()
	defer func() {
		releaseNight()
		closeCtx, cancelClose := testDeadlineContext(t)
		defer cancelClose()
		if err := rt.GracefulClose(closeCtx); err != nil {
			_ = rt.ForceClose()
			t.Errorf("close runtime: %v", err)
		}
	}()
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	waitForReworkCondition(t, ctx, func() error {
		if status := queryRunStatus(t, db, "run_rework_flow"); status == "failed" || status == "cancelled" {
			t.Fatalf("run terminated before security validator start: %s; %s", status, runtimeRunDiagnostic(rt, "run_rework_flow"))
		}
		select {
		case <-nightValidatorStarted:
			return nil
		default:
			return fmt.Errorf("security validator has not started")
		}
	})

	waitForReworkCondition(t, ctx, func() error {
		if status := queryRunStatus(t, db, "run_rework_flow"); status == "failed" || status == "cancelled" {
			t.Fatalf("run terminated before validation wave settled: %s; %s", status, runtimeRunDiagnostic(rt, "run_rework_flow"))
		}
		attempt, status, err := lookupLatestNodeAttemptStatus(db, "run_rework_flow", "implementation")
		if err != nil {
			return err
		}
		if attempt > 1 {
			t.Fatalf("implementation advanced to attempt %d before validation wave settled", attempt)
		}
		if attempt != 1 || status != "succeeded" {
			return fmt.Errorf("expected delegated execution to remain on attempt=1 status=succeeded until validation wave settles, got attempt=%d status=%s", attempt, status)
		}
		if got, err := lookupNodeAttemptStatusForNode(db, "run_rework_flow", "quality_validation", 1); err != nil {
			return err
		} else if got != "failed" {
			return fmt.Errorf("expected qa validator attempt 1 failed, got %s; %s", got, runtimeRunDiagnostic(rt, "run_rework_flow"))
		}
		if got, err := lookupNodeAttemptStatusForNode(db, "run_rework_flow", "security_validation", 1); err != nil {
			return err
		} else if got != "running" {
			return fmt.Errorf("expected nightraven validator attempt 1 still running, got %s", got)
		}
		return nil
	})
	var guardLifecycle string
	if err := db.QueryRowContext(ctx, `SELECT lifecycle FROM provider_process_guards WHERE attempt_id = 'run_rework_flow:security_validation:1'`).Scan(&guardLifecycle); err != nil {
		t.Fatalf("load blocked security guard: %v", err)
	}
	if guardLifecycle != "acknowledged" {
		t.Fatalf("blocked security guard lifecycle=%q, want acknowledged before release", guardLifecycle)
	}

	releaseNight()

	waitForReworkCondition(t, ctx, func() error {
		status := queryRunStatus(t, db, "run_rework_flow")
		if status != "succeeded" {
			if status == "failed" {
				var failedNodeID string
				var failedAttempt int
				var failedReason string
				latestStates := make([]string, 0, 5)
				_ = db.QueryRow(`SELECT node_id, attempt FROM node_runs WHERE run_id = 'run_rework_flow' AND node_status = 'failed' ORDER BY attempt DESC, node_id ASC LIMIT 1`).Scan(&failedNodeID, &failedAttempt)
				_ = db.QueryRow(`SELECT reason_code FROM events WHERE run_id = 'run_rework_flow' AND event_type = 'attempt.failed' ORDER BY created_at DESC, seq DESC LIMIT 1`).Scan(&failedReason)
				for _, nodeID := range []string{"implementation", "execution_review", "quality_validation", "security_validation", "completion"} {
					var attempt int
					var nodeStatus string
					if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_rework_flow' AND node_id = ? ORDER BY attempt DESC LIMIT 1`, nodeID).Scan(&attempt, &nodeStatus); err == nil {
						latestStates = append(latestStates, fmt.Sprintf("%s:%d:%s", nodeID, attempt, nodeStatus))
					}
				}
				t.Fatalf("run_rework_flow status=%s failed_node=%s attempt=%d reason=%s latest=%s; %s", status, failedNodeID, failedAttempt, failedReason, strings.Join(latestStates, ","), runtimeRunDiagnostic(rt, "run_rework_flow"))
			}
			if status == "cancelled" {
				t.Fatalf("run_rework_flow was cancelled before successful rework: %s", runtimeRunDiagnostic(rt, "run_rework_flow"))
			}
			return fmt.Errorf("run_rework_flow status=%s", status)
		}
		return nil
	})

	for _, tc := range []struct {
		nodeID  string
		attempt int
		status  string
	}{
		{nodeID: "implementation", attempt: 2, status: "succeeded"},
		{nodeID: "execution_review", attempt: 2, status: "succeeded"},
		{nodeID: "quality_validation", attempt: 2, status: "succeeded"},
		{nodeID: "security_validation", attempt: 2, status: "succeeded"},
		{nodeID: "completion", attempt: 1, status: "succeeded"},
	} {
		var got string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_rework_flow' AND node_id = ? AND attempt = ?`, tc.nodeID, tc.attempt).Scan(&got); err != nil {
			t.Fatalf("query node %s attempt %d status: %v", tc.nodeID, tc.attempt, err)
		}
		if got != tc.status {
			t.Fatalf("expected %s attempt %d status %q, got %q", tc.nodeID, tc.attempt, tc.status, got)
		}
	}
	if err := db.QueryRowContext(ctx, `SELECT lifecycle FROM provider_process_guards WHERE attempt_id = 'run_rework_flow:security_validation:1'`).Scan(&guardLifecycle); err != nil {
		t.Fatalf("load settled security guard: %v", err)
	}
	if guardLifecycle != "quiescent" {
		t.Fatalf("settled security guard lifecycle=%q, want quiescent", guardLifecycle)
	}
}

func waitForReworkCondition(t *testing.T, ctx context.Context, condition func() error) {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := condition()
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("rework condition did not settle before test cancellation: %v; %v", ctx.Err(), err)
		case <-ticker.C:
		}
	}
}

func runtimeRunDiagnostic(rt *Runtime, runID string) string {
	parts := make([]string, 0, 7)
	for _, query := range []string{
		`SELECT 'attempts=' || json_group_array(json_object('node',nr.node_id,'status',nr.node_status,'state',ae.state,'base',ae.base_snapshot_id,'result',ae.result_snapshot_id,'lease',ae.lease_owner,'failure',ae.failure_code)) FROM node_runs nr LEFT JOIN attempt_executions ae ON ae.node_run_id=nr.node_run_id WHERE nr.run_id=?`,
		`SELECT 'outbox=' || json_group_array(json_object('attempt',attempt_id,'status',status,'error',last_error)) FROM attempt_outbox WHERE run_id=?`,
		`SELECT 'handoffs=' || json_group_array(attempt_id) FROM provider_terminal_handoffs WHERE run_id=?`,
		`SELECT 'cancel=' || json_group_array(json_object('attempt',attempt_id,'status',status,'handoff',handoff_state)) FROM attempt_cancel_outbox WHERE run_id=?`,
		`SELECT 'guards=' || json_group_array(json_object('attempt',attempt_id,'state',lifecycle)) FROM provider_process_guards WHERE run_id=?`,
	} {
		var value string
		if err := rt.orchestrator.db.QueryRow(query, runID).Scan(&value); err != nil {
			value = err.Error()
		}
		parts = append(parts, value)
	}
	rt.orchestrator.execMu.Lock()
	parts = append(parts, fmt.Sprintf("maintenance=%v inflight=%v preparations=%d", rt.orchestrator.maintenance.active, rt.orchestrator.inflightRunIDs, len(rt.orchestrator.preparations)))
	rt.orchestrator.execMu.Unlock()
	return strings.Join(parts, " ")
}

func queryLatestNodeAttemptStatus(t *testing.T, db *sql.DB, runID, nodeID string) (int, string) {
	t.Helper()

	attempt, status, err := lookupLatestNodeAttemptStatus(db, runID, nodeID)
	if err != nil {
		t.Fatalf("query latest node attempt status run=%s node=%s: %v", runID, nodeID, err)
	}
	return attempt, status
}

func lookupLatestNodeAttemptStatus(db *sql.DB, runID, nodeID string) (int, string, error) {
	var attempt int
	var status string
	if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = ? AND node_id = ? ORDER BY attempt DESC LIMIT 1`, runID, nodeID).Scan(&attempt, &status); err != nil {
		return 0, "", err
	}
	return attempt, status, nil
}

func queryNodeAttemptStatus(t *testing.T, db *sql.DB, nodeRunID string) string {
	t.Helper()

	status, err := lookupNodeAttemptStatus(db, nodeRunID)
	if err != nil {
		t.Fatalf("query node status %s: %v", nodeRunID, err)
	}
	return status
}

func lookupNodeAttemptStatus(db *sql.DB, nodeRunID string) (string, error) {
	var status string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = ?`, nodeRunID).Scan(&status); err != nil {
		return "", err
	}
	return status, nil
}

func lookupNodeAttemptStatusForNode(db *sql.DB, runID, nodeID string, attempt int) (string, error) {
	var status string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = ? AND node_id = ? AND attempt = ?`, runID, nodeID, attempt).Scan(&status); err != nil {
		return "", err
	}
	return status, nil
}

func seedValidationReworkFlowRun(t *testing.T, db *sql.DB) {
	t.Helper()

	createdAt := "2026-03-01T12:00:00Z"
	for _, roleID := range []string{
		"governance",
		"plan-author",
		"builder",
		"reviewer",
		"qa-validator",
		"security-validator",
	} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('%s', '%s', 0, '%s')`, roleID, roleID, createdAt))
	}
	mustExec(t, db, `UPDATE roles SET can_spawn = 1 WHERE role_id IN ('governance', 'plan-author')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_rework_flow', 'rework flow', 'validation rework flow', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_rework_flow', 'rework-flow', 'v1', '{"version":"v1","workflow_id":"rework-flow","nodes":[],"edges":[],"metadata":{"max_validation_rework_cycles":2}}', '2026-03-01T12:00:00Z')`)
	for _, stmt := range []string{
		`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_kickoff', 'wfdef_rework_flow', 'execution_kickoff', 'plan-author', 'execution_kickoff', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_exec', 'wfdef_rework_flow', 'implementation', 'builder', 'delegated_execution', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_review', 'wfdef_rework_flow', 'execution_review', 'reviewer', 'execution_review', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_validator_qa', 'wfdef_rework_flow', 'quality_validation', 'qa-validator', 'validation_gates', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_validator_night', 'wfdef_rework_flow', 'security_validation', 'security-validator', 'validation_gates', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_closure', 'wfdef_rework_flow', 'completion', 'governance', 'completion', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_kickoff_exec', 'wfdef_rework_flow', 'execution_kickoff', 'implementation', 'success', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_exec_review', 'wfdef_rework_flow', 'implementation', 'execution_review', 'success', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_review_qa', 'wfdef_rework_flow', 'execution_review', 'quality_validation', 'success', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_review_night', 'wfdef_rework_flow', 'execution_review', 'security_validation', 'success', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_qa_closure', 'wfdef_rework_flow', 'quality_validation', 'completion', 'success', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_night_closure', 'wfdef_rework_flow', 'security_validation', 'completion', 'success', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_rework_flow', NULL, 'work_order_rework_flow', 'wfdef_rework_flow', 'pending', 'approved', 2, 2, 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`,
	} {
		mustExec(t, db, stmt)
	}
}
