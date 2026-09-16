package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	coreworkflow "kingdom/internal/core/workflow"
	"kingdom/internal/nodeattempt"
)

// RunNodeExecutionRecord is the durable, canonical projection of a workflow
// node. Current is intentionally nil when the visible node attempt has not
// produced a physical execution row yet.
type RunNodeExecutionRecord struct {
	NodeID             string
	RequiredRole       string
	NodeClass          string
	ExecutionOwnership ExecutionMode
	NodeStatus         string
	VisibleNodeRunID   sql.NullString
	VisibleAttempt     sql.NullInt64
	Current            *RunExecutionRecord
	AttemptCount       int
	LatestActivityAt   sql.NullString
	Recovery           *RunRecoveryRecord
}

// ReadQuerier is the read-only database capability needed by logical-node
// projections. It permits callers that already own a read transaction to use
// the same canonical projection snapshot.
type ReadQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// RunRecoveryRecord comes exclusively from durable recovery columns and retry
// decisions. It never reconstructs recovery state from event payloads.
type RunRecoveryRecord struct {
	Phase           string
	Trigger         string
	ReasonCode      string
	SourceAttemptID string
	SourceNodeRunID string
	TargetAttemptID sql.NullString
	TargetNodeRunID sql.NullString
	RetryIndex      int
	RetryMax        int
	ScheduledAt     string
	LastUpdatedAt   string
	OperatorText    sql.NullString
}

// ListRunNodeExecutions returns exactly one visible logical projection for
// every workflow node. The query is bounded by the workflow node set and does
// not issue per-node reads.
func (s *Store) ListRunNodeExecutions(ctx context.Context, runID string) ([]RunNodeExecutionRecord, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store database is required")
	}
	return ListRunNodeExecutions(ctx, s.db, runID)
}

// ListRunNodeExecutions returns exactly one visible logical projection for
// every workflow node using the supplied read capability.
func ListRunNodeExecutions(ctx context.Context, q ReadQuerier, runID string) ([]RunNodeExecutionRecord, error) {
	if q == nil {
		return nil, fmt.Errorf("store database is required")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("run_id is required")
	}

	query := fmt.Sprintf(`WITH ranked_node_runs AS (
		SELECT nr.node_id, nr.node_run_id, nr.attempt, nr.node_status,
		       ROW_NUMBER() OVER (PARTITION BY nr.node_id ORDER BY %s) AS node_rank
		FROM node_runs nr
		WHERE nr.run_id = ?
	), visible_node_runs AS (
		SELECT node_id, node_run_id, attempt, node_status
		FROM ranked_node_runs
		WHERE node_rank = 1
	), attempt_counts AS (
		SELECT nr.node_id, COUNT(ae.attempt_id) AS attempt_count
		FROM node_runs nr
		LEFT JOIN attempt_executions ae ON ae.run_id = nr.run_id AND ae.node_run_id = nr.node_run_id
		WHERE nr.run_id = ?
		GROUP BY nr.node_id
	), logical_activity_source AS (
		SELECT nr.node_id, COALESCE(nr.finished_at, nr.started_at) AS activity_at
		FROM node_runs nr WHERE nr.run_id = ?
		UNION ALL
		SELECT nr.node_id, COALESCE(ae.updated_at, ae.started_at, ae.launch_started_at, ae.created_at)
		FROM attempt_executions ae JOIN node_runs nr ON nr.run_id = ae.run_id AND nr.node_run_id = ae.node_run_id
		WHERE ae.run_id = ?
		UNION ALL
		SELECT rd.node_id, COALESCE(rd.resolved_at, rd.dispatched_at, rd.created_at)
		FROM retry_decisions rd WHERE rd.subject_kind = 'decree_attempt' AND rd.run_id = ?
	), logical_activity AS (
		SELECT node_id, MAX(activity_at) AS latest_activity_at
		FROM logical_activity_source
		WHERE activity_at IS NOT NULL AND TRIM(activity_at) <> ''
		GROUP BY node_id
	), recovery_candidates AS (
		SELECT rd.node_id,
		       CASE
		         WHEN rd.decision IN ('exhausted', 'fail_fast') OR rd.resolution = 'escalated' THEN 'exhausted'
		         WHEN rd.resolution = 'recovered' THEN 'recovered'
		         WHEN rd.dispatch_state = 'dispatched' AND rd.resolution = 'pending' THEN 'running'
		         WHEN source_nr.node_status = 'blocked' THEN 'paused'
		         ELSE 'scheduled'
		       END AS phase,
		       'automatic_retry' AS trigger,
		       rd.error_code AS reason_code,
		       rd.source_attempt_id,
		       rd.node_run_id AS source_node_run_id,
		       target_ae.attempt_id AS target_attempt_id,
		       rd.target_node_run_id,
		       rd.retry_index,
		       rd.max_retries AS retry_max,
		       rd.due_at AS scheduled_at,
		       COALESCE(rd.resolved_at, rd.dispatched_at, rd.created_at) AS last_updated_at,
		       rd.operator_text,
		       ROW_NUMBER() OVER (
		         PARTITION BY rd.node_id
		         ORDER BY COALESCE(rd.resolved_at, rd.dispatched_at, rd.created_at) DESC, rd.retry_index DESC, rd.retry_decision_id DESC
		       ) AS recovery_rank
		FROM retry_decisions rd
		JOIN node_runs source_nr ON source_nr.run_id = rd.run_id AND source_nr.node_run_id = rd.node_run_id
		LEFT JOIN attempt_executions target_ae ON target_ae.run_id = rd.run_id AND target_ae.node_run_id = rd.target_node_run_id
		WHERE rd.subject_kind = 'decree_attempt' AND rd.run_id = ?
	), manual_recovery_candidates AS (
		SELECT nr.node_id,
		       CASE
		         WHEN nr.node_status IN ('starting', 'running') THEN 'running'
		         WHEN nr.node_status IN ('succeeded', 'skipped') THEN 'recovered'
		         WHEN nr.node_status IN ('failed', 'cancelled') THEN 'exhausted'
		         ELSE 'scheduled'
		       END AS phase,
		       nrec.trigger,
		       nrec.reason_code,
		       nrec.source_attempt_id,
		       nrec.source_node_run_id,
		       target_ae.attempt_id AS target_attempt_id,
		       nr.node_run_id AS target_node_run_id,
		       0 AS retry_index,
		       0 AS retry_max,
		       nrec.scheduled_at AS scheduled_at,
		       nrec.scheduled_at AS last_updated_at,
		       NULL AS operator_text,
		       ROW_NUMBER() OVER (
		         PARTITION BY nr.node_id
		         ORDER BY nrec.scheduled_at DESC, nr.attempt DESC, nr.node_run_id DESC
		       ) AS recovery_rank
		FROM node_attempt_recoveries nrec
		JOIN node_runs nr ON nr.run_id = nrec.run_id AND nr.node_run_id = nrec.target_node_run_id
		LEFT JOIN attempt_executions target_ae ON target_ae.run_id = nr.run_id AND target_ae.node_run_id = nr.node_run_id
		WHERE nrec.run_id = ?
	), ranked_recovery AS (
		SELECT * FROM recovery_candidates WHERE recovery_rank = 1
		UNION ALL
		SELECT * FROM manual_recovery_candidates WHERE recovery_rank = 1
	), current_recovery AS (
		SELECT *, ROW_NUMBER() OVER (
		  PARTITION BY node_id ORDER BY last_updated_at DESC, target_node_run_id DESC
		) AS current_rank
		FROM ranked_recovery
	)
	SELECT wn.node_id, wn.required_role, wn.node_class,
	       vnr.node_run_id, vnr.attempt, COALESCE(vnr.node_status, 'pending'), COALESCE(ac.attempt_count, 0), la.latest_activity_at,
	       ae.attempt_id, COALESCE(ae.run_id, ''), COALESCE(ae.node_run_id, ''), COALESCE(nr.node_id, ''), COALESCE(nr.attempt, 0), COALESCE(nr.node_status, ''), COALESCE(nr.required_role, ''), COALESCE(nr.agent_id, ''),
	       COALESCE(ae.state, ''), COALESCE(ae.execution_mode, ''), NULLIF(TRIM(ae.session_ref), ''), NULLIF(TRIM(ae.provider_session_id), ''), COALESCE(ae.created_at, ''),
	       COALESCE(ae.updated_at, ae.started_at, ae.launch_started_at, ae.created_at, ''), NULLIF(TRIM(ae.progress_event_type), ''),
	       NULLIF(TRIM(ae.progress_reason_code), ''), ae.progress_at, ae.launch_started_at, ae.started_at, COALESCE(ae.updated_at, ''),
	       ae.terminal_observed_at, ae.committed_at, NULLIF(TRIM(ae.failure_code), ''), NULLIF(TRIM(ae.failure_text), ''),
	       COALESCE(NULLIF(TRIM(cpi.invalid_reason), ''), NULLIF(TRIM(api.invalid_reason), '')),
	       EXISTS(SELECT 1 FROM attempt_cancel_outbox aco WHERE aco.attempt_id = ae.attempt_id AND aco.status = 'pending'),
	       (SELECT NULLIF(TRIM(aco.handoff_state), '') FROM attempt_cancel_outbox aco WHERE aco.attempt_id = ae.attempt_id AND aco.status = 'pending'),
	       CASE WHEN nr.node_status = 'blocked' AND ae.state = 'committed_cancelled' AND EXISTS(
		   SELECT 1 FROM attempt_cancel_outbox aco
		   JOIN provider_process_guards ppg ON ppg.attempt_id = aco.attempt_id AND ppg.owner_kind = 'decree_attempt' AND ppg.lifecycle = 'acknowledged'
		   WHERE aco.attempt_id = ae.attempt_id AND aco.status = 'pending' AND aco.reason_code = 'agent_unresponsive'
		     AND aco.handoff_state IN ('awaiting_quiescence', 'cancellation_dispatched', 'stop_retry_dispatched')
		 ) THEN 1 ELSE 0 END,
	       COALESCE(cr.phase, ''), COALESCE(cr.trigger, ''), COALESCE(cr.reason_code, ''), COALESCE(cr.source_attempt_id, ''), COALESCE(cr.source_node_run_id, ''), cr.target_attempt_id,
	       cr.target_node_run_id, COALESCE(cr.retry_index, 0), COALESCE(cr.retry_max, 0), COALESCE(cr.scheduled_at, ''), COALESCE(cr.last_updated_at, ''), cr.operator_text
	FROM workflow_runs wr
	JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id
	LEFT JOIN visible_node_runs vnr ON vnr.node_id = wn.node_id
	LEFT JOIN attempt_counts ac ON ac.node_id = wn.node_id
	LEFT JOIN logical_activity la ON la.node_id = wn.node_id
	LEFT JOIN node_runs nr ON nr.run_id = wr.run_id AND nr.node_run_id = vnr.node_run_id
	LEFT JOIN attempt_executions ae ON ae.run_id = wr.run_id AND ae.node_run_id = vnr.node_run_id
	LEFT JOIN provider_completion_settlements pcs ON pcs.settlement_id = ae.canonical_settlement_id
	LEFT JOIN provider_invocations api ON api.provider_invocation_id = ae.active_provider_invocation_id
	LEFT JOIN provider_invocations cpi ON cpi.provider_invocation_id = pcs.provider_invocation_id
	LEFT JOIN current_recovery cr ON cr.node_id = wn.node_id AND cr.current_rank = 1
	WHERE wr.run_id = ?
	ORDER BY CASE COALESCE(vnr.node_status, 'pending')
		WHEN 'starting' THEN 1
		WHEN 'running' THEN 1
		WHEN 'blocked' THEN 2
		WHEN 'pending' THEN 3
		WHEN 'ready' THEN 3
		ELSE 4
	END ASC,
	la.latest_activity_at DESC,
	wn.node_id ASC`, nodeattempt.VisibleNodeAttemptOrderSQL("nr"))

	rows, err := q.QueryContext(ctx, query, runID, runID, runID, runID, runID, runID, runID, runID)
	if err != nil {
		return nil, fmt.Errorf("list logical node executions for %q: %w", runID, err)
	}
	defer rows.Close()

	records := make([]RunNodeExecutionRecord, 0)
	for rows.Next() {
		var record RunNodeExecutionRecord
		var currentAttemptID sql.NullString
		var current RunExecutionRecord
		var currentExecutionMode string
		var recovery RunRecoveryRecord
		if err := rows.Scan(
			&record.NodeID, &record.RequiredRole, &record.NodeClass, &record.VisibleNodeRunID, &record.VisibleAttempt, &record.NodeStatus, &record.AttemptCount, &record.LatestActivityAt,
			&currentAttemptID, &current.RunID, &current.NodeRunID, &current.NodeID, &current.Attempt, &current.NodeStatus, &current.RequiredRole, &current.AgentID,
			&current.State, &currentExecutionMode, &current.LogicalSessionID, &current.ProviderSessionID, &current.QueuedAt, &current.ActivityAt,
			&current.ProgressEventType, &current.ProgressReasonCode, &current.ProgressAt, &current.LaunchStartedAt, &current.StartedAt, &current.UpdatedAt,
			&current.TerminalObservedAt, &current.CommittedAt, &current.FailureCode, &current.FailureText, &current.CompletionInvalidReason,
			&current.CancellationPending, &current.CancellationHandoffState, &current.StopRetryAvailable,
			&recovery.Phase, &recovery.Trigger, &recovery.ReasonCode, &recovery.SourceAttemptID, &recovery.SourceNodeRunID, &recovery.TargetAttemptID,
			&recovery.TargetNodeRunID, &recovery.RetryIndex, &recovery.RetryMax, &recovery.ScheduledAt, &recovery.LastUpdatedAt, &recovery.OperatorText,
		); err != nil {
			return nil, fmt.Errorf("scan logical node execution for %q: %w", runID, err)
		}
		record.ExecutionOwnership = executionOwnershipForNode(record.NodeID, record.NodeClass)
		if currentAttemptID.Valid {
			current.AttemptID = currentAttemptID.String
			current.ExecutionMode = NormalizeExecutionMode(currentExecutionMode)
			record.Current = &current
		}
		if strings.TrimSpace(recovery.Phase) != "" {
			record.Recovery = &recovery
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate logical node executions for %q: %w", runID, err)
	}
	return records, nil
}

// CurrentRunNodeExecution returns only the currently visible physical lineage
// for one logical node. It uses the same visible-node selector as the complete
// logical projection without loading the rest of the workflow.
func (s *Store) CurrentRunNodeExecution(ctx context.Context, runID, nodeID string) (sql.NullString, sql.NullString, error) {
	if s == nil || s.db == nil {
		return sql.NullString{}, sql.NullString{}, fmt.Errorf("store database is required")
	}
	runID = strings.TrimSpace(runID)
	nodeID = strings.TrimSpace(nodeID)
	if runID == "" || nodeID == "" {
		return sql.NullString{}, sql.NullString{}, fmt.Errorf("run_id and node_id are required")
	}

	query := fmt.Sprintf(`SELECT nr.node_run_id, ae.attempt_id
		FROM node_runs nr
		LEFT JOIN attempt_executions ae ON ae.run_id = nr.run_id AND ae.node_run_id = nr.node_run_id
		WHERE nr.run_id = ? AND nr.node_id = ?
		ORDER BY %s
		LIMIT 1`, nodeattempt.VisibleNodeAttemptOrderSQL("nr"))
	var nodeRunID sql.NullString
	var attemptID sql.NullString
	err := s.db.QueryRowContext(ctx, query, runID, nodeID).Scan(&nodeRunID, &attemptID)
	if errors.Is(err, sql.ErrNoRows) {
		return sql.NullString{}, sql.NullString{}, nil
	}
	if err != nil {
		return sql.NullString{}, sql.NullString{}, fmt.Errorf("get current node execution for run=%q node=%q: %w", runID, nodeID, err)
	}
	return nodeRunID, attemptID, nil
}

func executionOwnershipForNode(nodeID, nodeClass string) ExecutionMode {
	if coreworkflow.BehaviorForNode(nodeID, nodeClass).ExecutionOwnership == coreworkflow.NodeExecutionOwnershipRuntimeOwned {
		return ExecutionModeRuntimeOwned
	}
	return ExecutionModeProviderBacked
}

type RunNodeAttemptsPage struct {
	Items []RunExecutionRecord
}

// ListRunNodeAttemptsPage returns bounded physical execution history in the
// immutable node-attempt order used by its cursor.
func (s *Store) ListRunNodeAttemptsPage(ctx context.Context, runID, nodeID string, beforeAttempt int, beforeNodeRunID string, limit int) (RunNodeAttemptsPage, error) {
	if s == nil || s.db == nil {
		return RunNodeAttemptsPage{}, fmt.Errorf("store database is required")
	}
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(nodeID) == "" || limit < 1 {
		return RunNodeAttemptsPage{}, fmt.Errorf("run_id, node_id, and positive limit are required")
	}
	if beforeAttempt < 0 {
		return RunNodeAttemptsPage{}, fmt.Errorf("cursor attempt must be non-negative")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT ae.attempt_id, ae.run_id, ae.node_run_id, nr.node_id, nr.attempt, nr.node_status,
		nr.required_role, nr.agent_id, ae.state, ae.execution_mode, NULLIF(TRIM(ae.session_ref), ''),
		NULLIF(TRIM(ae.provider_session_id), ''), ae.created_at, COALESCE(ae.updated_at, ae.started_at, ae.launch_started_at, ae.created_at),
		NULLIF(TRIM(ae.progress_event_type), ''), NULLIF(TRIM(ae.progress_reason_code), ''), ae.progress_at, ae.launch_started_at,
		ae.started_at, ae.updated_at, ae.terminal_observed_at, ae.committed_at, NULLIF(TRIM(ae.failure_code), ''),
		NULLIF(TRIM(ae.failure_text), ''), COALESCE(NULLIF(TRIM(cpi.invalid_reason), ''), NULLIF(TRIM(api.invalid_reason), '')),
		EXISTS(SELECT 1 FROM attempt_cancel_outbox aco WHERE aco.attempt_id = ae.attempt_id AND aco.status = 'pending'),
		(SELECT NULLIF(TRIM(aco.handoff_state), '') FROM attempt_cancel_outbox aco WHERE aco.attempt_id = ae.attempt_id AND aco.status = 'pending'),
		CASE WHEN nr.node_status = 'blocked' AND ae.state = 'committed_cancelled' AND EXISTS(
			SELECT 1 FROM attempt_cancel_outbox aco
			JOIN provider_process_guards ppg ON ppg.attempt_id = aco.attempt_id AND ppg.owner_kind = 'decree_attempt' AND ppg.lifecycle = 'acknowledged'
			WHERE aco.attempt_id = ae.attempt_id AND aco.status = 'pending' AND aco.reason_code = 'agent_unresponsive'
			  AND aco.handoff_state IN ('awaiting_quiescence', 'cancellation_dispatched', 'stop_retry_dispatched')
		) THEN 1 ELSE 0 END
		FROM attempt_executions ae
		JOIN node_runs nr ON nr.node_run_id = ae.node_run_id AND nr.run_id = ae.run_id
		LEFT JOIN provider_completion_settlements pcs ON pcs.settlement_id = ae.canonical_settlement_id
		LEFT JOIN provider_invocations api ON api.provider_invocation_id = ae.active_provider_invocation_id
		LEFT JOIN provider_invocations cpi ON cpi.provider_invocation_id = pcs.provider_invocation_id
		WHERE ae.run_id = ? AND nr.node_id = ?
		  AND (? = 0 OR nr.attempt < ? OR (nr.attempt = ? AND nr.node_run_id < ?))
		ORDER BY nr.attempt DESC, nr.node_run_id DESC
		LIMIT ?`, strings.TrimSpace(runID), strings.TrimSpace(nodeID), beforeAttempt, beforeAttempt, beforeAttempt, strings.TrimSpace(beforeNodeRunID), limit)
	if err != nil {
		return RunNodeAttemptsPage{}, fmt.Errorf("list node attempts for run=%q node=%q: %w", runID, nodeID, err)
	}
	defer rows.Close()
	page := RunNodeAttemptsPage{Items: make([]RunExecutionRecord, 0, limit)}
	for rows.Next() {
		var record RunExecutionRecord
		var executionMode string
		if err := rows.Scan(&record.AttemptID, &record.RunID, &record.NodeRunID, &record.NodeID, &record.Attempt, &record.NodeStatus,
			&record.RequiredRole, &record.AgentID, &record.State, &executionMode, &record.LogicalSessionID, &record.ProviderSessionID,
			&record.QueuedAt, &record.ActivityAt, &record.ProgressEventType, &record.ProgressReasonCode, &record.ProgressAt, &record.LaunchStartedAt,
			&record.StartedAt, &record.UpdatedAt, &record.TerminalObservedAt, &record.CommittedAt, &record.FailureCode, &record.FailureText,
			&record.CompletionInvalidReason, &record.CancellationPending, &record.CancellationHandoffState, &record.StopRetryAvailable); err != nil {
			return RunNodeAttemptsPage{}, fmt.Errorf("scan node attempt for run=%q node=%q: %w", runID, nodeID, err)
		}
		record.ExecutionMode = NormalizeExecutionMode(executionMode)
		page.Items = append(page.Items, record)
	}
	if err := rows.Err(); err != nil {
		return RunNodeAttemptsPage{}, fmt.Errorf("iterate node attempts for run=%q node=%q: %w", runID, nodeID, err)
	}
	return page, nil
}
