package orchestration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/providers/opencode"
	"kingdom/internal/testutil"
)

type scriptedExecutionStep struct {
	output string
	err    error
	block  <-chan struct{}
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

func (a *scriptedExecutionAdapter) Execute(ctx context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	a.mu.Lock()
	a.requests = append(a.requests, req)
	step, ok := a.steps[req.AttemptID]
	a.mu.Unlock()
	if !ok {
		return opencode.ExecuteResponse{Accepted: true, OutputText: `{"summary":"ok"}`}, nil
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
	response := opencode.ExecuteResponse{Accepted: true, OutputText: step.output}
	if req.OnCompletionReceipt != nil && strings.TrimSpace(req.InvocationID) != "" && strings.TrimSpace(req.CompletionCapability) != "" && strings.TrimSpace(step.output) != "" {
		acceptance, err := req.OnCompletionReceipt(opencode.CompletionReceipt{
			ProtocolVersion:       opencode.CompletionProtocolVersion,
			AttemptID:             req.AttemptID,
			InvocationID:          req.InvocationID,
			CompletionCapability:  req.CompletionCapability,
			FinalText:             step.output,
			StructuredPayloadJSON: scriptedStructuredPayload(step.output),
		})
		if err != nil {
			return opencode.ExecuteResponse{}, err
		}
		response.CompletionReceiptAccepted = true
		response.CompletionReceiptEvidenceID = acceptance.EvidenceID
		response.CompletionStructuredPayloadJSON = acceptance.StructuredPayloadJSON
		response.ProviderInvocationID = req.InvocationID
		response.ProviderInvocationAt = req.InvocationIssuedAt
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

func scriptedStructuredPayload(output string) string {
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		switch trimmed {
		case "VERDICT: PASS":
			return `{"verdict":"PASS"}`
		case "VERDICT: REWORK":
			return `{"verdict":"REWORK"}`
		default:
			return `{}`
		}
	}
	return `{}`
}

func TestRuntimeDispatchLoopValidationReworkWaitsForWaveSettlementAndSucceeds(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedValidationReworkFlowRun(t, db)

	releaseNightValidator := make(chan struct{})
	var releaseNightValidatorOnce sync.Once
	releaseNight := func() {
		releaseNightValidatorOnce.Do(func() {
			close(releaseNightValidator)
		})
	}
	adapter := newScriptedExecutionAdapter(map[string]scriptedExecutionStep{
		"run_rework_flow:execution_kickoff:1":             {output: "Kickoff confirmed."},
		"run_rework_flow:delegated_execution_berserker:1": {output: "Initial implementation complete."},
		"run_rework_flow:execution_review_shieldmaiden:1": {output: "VERDICT: PASS\nInitial review looks good."},
		"run_rework_flow:validation_gates_qa_scout:1":     {output: "VERDICT: REWORK\nFix the validator blocker."},
		"run_rework_flow:validation_gates_nightraven:1":   {output: "VERDICT: PASS\nNight watch checks are green.", block: releaseNightValidator},
		"run_rework_flow:delegated_execution_berserker:2": {output: "Implemented follow-up fixes."},
		"run_rework_flow:execution_review_shieldmaiden:2": {output: "VERDICT: PASS\nFollow-up review is clean."},
		"run_rework_flow:validation_gates_qa_scout:2":     {output: "VERDICT: PASS\nQA checks are green."},
		"run_rework_flow:validation_gates_nightraven:2":   {output: "VERDICT: PASS\nNight watch confirms the fix."},
		"run_rework_flow:royal_closure:1":                 {output: "Royal closure recorded."},
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		releaseNight()
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	testutil.Eventually(t, 8*time.Second, 20*time.Millisecond, func() error {
		attempt, status, err := lookupLatestNodeAttemptStatus(db, "run_rework_flow", "delegated_execution_berserker")
		if err != nil {
			return err
		}
		if attempt != 1 || status != "succeeded" {
			return fmt.Errorf("expected delegated execution to remain on attempt=1 status=succeeded until validation wave settles, got attempt=%d status=%s", attempt, status)
		}
		if got, err := lookupNodeAttemptStatusForNode(db, "run_rework_flow", "validation_gates_qa_scout", 1); err != nil {
			return err
		} else if got != "failed" {
			return fmt.Errorf("expected qa validator attempt 1 failed, got %s", got)
		}
		if got, err := lookupNodeAttemptStatusForNode(db, "run_rework_flow", "validation_gates_nightraven", 1); err != nil {
			return err
		} else if got != "running" {
			return fmt.Errorf("expected nightraven validator attempt 1 still running, got %s", got)
		}
		return nil
	})

	releaseNight()

	awaitRunStatus(t, db, "run_rework_flow", 12*time.Second, 25*time.Millisecond, func(status string) error {
		if status != "succeeded" {
			if status == "failed" {
				var failedNodeID string
				var failedAttempt int
				var failedReason string
				_ = db.QueryRow(`SELECT node_id, attempt FROM node_runs WHERE run_id = 'run_rework_flow' AND node_status = 'failed' ORDER BY attempt DESC, node_id ASC LIMIT 1`).Scan(&failedNodeID, &failedAttempt)
				_ = db.QueryRow(`SELECT reason_code FROM events WHERE run_id = 'run_rework_flow' AND event_type = 'attempt.failed' ORDER BY created_at DESC, seq DESC LIMIT 1`).Scan(&failedReason)
				return fmt.Errorf("run_rework_flow status=%s failed_node=%s attempt=%d reason=%s", status, failedNodeID, failedAttempt, failedReason)
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
		{nodeID: "delegated_execution_berserker", attempt: 2, status: "succeeded"},
		{nodeID: "execution_review_shieldmaiden", attempt: 2, status: "succeeded"},
		{nodeID: "validation_gates_qa_scout", attempt: 2, status: "succeeded"},
		{nodeID: "validation_gates_nightraven", attempt: 2, status: "succeeded"},
		{nodeID: "royal_closure", attempt: 1, status: "succeeded"},
	} {
		var got string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_rework_flow' AND node_id = ? AND attempt = ?`, tc.nodeID, tc.attempt).Scan(&got); err != nil {
			t.Fatalf("query node %s attempt %d status: %v", tc.nodeID, tc.attempt, err)
		}
		if got != tc.status {
			t.Fatalf("expected %s attempt %d status %q, got %q", tc.nodeID, tc.attempt, tc.status, got)
		}
	}
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
		"king",
		"council-member",
		"builder-berserker",
		"reviewer-shieldmaiden",
		"intelligence-qa-scout",
		"intelligence-nightraven",
	} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('%s', '%s', 0, '%s')`, roleID, roleID, createdAt))
	}
	mustExec(t, db, `UPDATE roles SET can_spawn = 1 WHERE role_id IN ('king', 'council-member')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_rework_flow', 'rework flow', 'validation rework flow', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_rework_flow', 'rework-flow', 'v1', '{"version":"v1","workflow_id":"rework-flow","nodes":[],"edges":[],"metadata":{"max_validation_rework_cycles":2}}', '2026-03-01T12:00:00Z')`)
	for _, stmt := range []string{
		`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_kickoff', 'wfdef_rework_flow', 'execution_kickoff', 'council-member', 'execution_kickoff', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_exec', 'wfdef_rework_flow', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_review', 'wfdef_rework_flow', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_validator_qa', 'wfdef_rework_flow', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_validator_night', 'wfdef_rework_flow', 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_closure', 'wfdef_rework_flow', 'royal_closure', 'king', 'royal_closure', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_kickoff_exec', 'wfdef_rework_flow', 'execution_kickoff', 'delegated_execution_berserker', 'success', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_exec_review', 'wfdef_rework_flow', 'delegated_execution_berserker', 'execution_review_shieldmaiden', 'success', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_review_qa', 'wfdef_rework_flow', 'execution_review_shieldmaiden', 'validation_gates_qa_scout', 'success', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_review_night', 'wfdef_rework_flow', 'execution_review_shieldmaiden', 'validation_gates_nightraven', 'success', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_qa_closure', 'wfdef_rework_flow', 'validation_gates_qa_scout', 'royal_closure', 'success', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_night_closure', 'wfdef_rework_flow', 'validation_gates_nightraven', 'royal_closure', 'success', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_rework_flow', NULL, 'decree_rework_flow', 'wfdef_rework_flow', 'pending', 'approved', 2, 2, 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`,
	} {
		mustExec(t, db, stmt)
	}
}
