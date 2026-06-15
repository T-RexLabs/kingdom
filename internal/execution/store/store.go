// Package store persists durable attempt execution state.
//
// It manages enqueueing, execution leases, ordered attempt-event writes,
// terminal commit bookkeeping, and the outbox handoff that lets orchestration
// project provider completion back into canonical run and node state without
// losing auditability.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/core/verdict"
)

type Store struct {
	db *sql.DB
}

type ExecutionMode string

const (
	ExecutionModeProviderBacked ExecutionMode = "provider_backed"
	ExecutionModeRuntimeOwned   ExecutionMode = "runtime_owned"
)

func NormalizeExecutionMode(raw string) ExecutionMode {
	switch ExecutionMode(strings.TrimSpace(raw)) {
	case ExecutionModeRuntimeOwned:
		return ExecutionModeRuntimeOwned
	default:
		return ExecutionModeProviderBacked
	}
}

func AttemptLaunchObserved(state string, startedAt string) bool {
	if strings.TrimSpace(startedAt) != "" {
		return true
	}
	return strings.TrimSpace(state) == "running"
}

func AttemptReachedLaunchHandoff(state string, launchStartedAt string, startedAt string, providerSessionID string, activeProviderInvocationID string) bool {
	if AttemptHasActiveLaunchFence(state, launchStartedAt, startedAt) || AttemptLaunchObserved(state, startedAt) {
		return true
	}
	return strings.TrimSpace(providerSessionID) != "" || strings.TrimSpace(activeProviderInvocationID) != ""
}

func AttemptHasActiveLaunchFence(state string, launchStartedAt string, startedAt string) bool {
	return strings.TrimSpace(state) == "starting" && strings.TrimSpace(launchStartedAt) != "" && strings.TrimSpace(startedAt) == ""
}

func AttemptRetainsSessionLease(state string, launchStartedAt string, startedAt string) bool {
	return AttemptHasActiveLaunchFence(state, launchStartedAt, startedAt) || AttemptLaunchObserved(state, startedAt)
}

func AttemptRenewsSessionLease(state string, launchStartedAt string, startedAt string) bool {
	return AttemptHasActiveLaunchFence(state, launchStartedAt, startedAt) || AttemptLaunchObserved(state, startedAt)
}

func AttemptInPreLaunch(state string, startedAt string) bool {
	return strings.TrimSpace(state) == "starting" && !AttemptLaunchObserved(state, startedAt)
}

type AttemptExecution struct {
	AttemptID                  string
	RunID                      string
	NodeRunID                  string
	State                      string
	ExecutionMode              ExecutionMode
	ProviderTimeoutMillis      int64
	SessionRef                 string
	ProviderSessionID          string
	ActiveProviderInvocationID string
	CanonicalSettlementID      string
	LaunchStartedAt            string
	StartedAt                  string
	TerminalObservedAt         string
	CommittedAt                string
	FailureCode                string
	FailureText                string
	ProgressEventType          string
	ProgressReasonCode         string
	ProgressAt                 string
}

type RunExecutionRecord struct {
	AttemptID          string
	RunID              string
	NodeRunID          string
	NodeID             string
	Attempt            int
	NodeStatus         string
	RequiredRole       string
	AgentID            string
	State              string
	ExecutionMode      ExecutionMode
	LogicalSessionID   sql.NullString
	ProviderSessionID  sql.NullString
	QueuedAt           string
	ActivityAt         string
	ProgressEventType  sql.NullString
	ProgressReasonCode sql.NullString
	ProgressAt         sql.NullString
	LaunchStartedAt    sql.NullString
	StartedAt          sql.NullString
	UpdatedAt          string
	TerminalObservedAt sql.NullString
	CommittedAt        sql.NullString
	FailureCode        sql.NullString
	FailureText        sql.NullString
}

type RunExecutionDetailRecord struct {
	RunExecutionRecord
	ProviderInvocationID           sql.NullString
	ProviderInvocationAt           sql.NullString
	ObservedOutputText             sql.NullString
	ObservedControlVerdict         sql.NullString
	CanonicalSettlementID          sql.NullString
	CanonicalSettlementOutcome     sql.NullString
	CanonicalSettlementAt          sql.NullString
	CanonicalStructuredPayloadJSON sql.NullString
}

type RunExecutionOutputRecord struct {
	AttemptID          string
	RunID              string
	NodeRunID          string
	NodeID             string
	NodeClass          string
	Attempt            int
	RequiredRole       string
	AgentID            string
	AgentDisplayName   sql.NullString
	State              string
	FailureCode        sql.NullString
	FailureText        sql.NullString
	OutputText         sql.NullString
	OutcomeReasonCode  sql.NullString
	LogicalSessionID   sql.NullString
	ProviderSessionID  sql.NullString
	CommittedAt        sql.NullString
	TerminalObservedAt sql.NullString
}

type ListRunExecutionOutputsRequest struct {
	RunID       string
	NodeClasses []string
	States      []string
}

type runExecutionPayloadSnapshot struct {
	ProviderInvocationID   sql.NullString
	ProviderInvocationAt   sql.NullString
	ObservedOutputText     sql.NullString
	ObservedControlVerdict sql.NullString
	OutcomeReasonCode      sql.NullString
}

type OutboxRecord struct {
	OutboxID      string
	AttemptID     string
	RunID         string
	NodeRunID     string
	TerminalSeq   int
	TerminalState string
	AttemptState  string
	PayloadJSON   string
	Status        string
	PublishCount  int
	FailureCode   string
	FailureText   string
}

type AttemptCancelRecord struct {
	CancelOutboxID string
	AttemptID      string
	RunID          string
	NodeRunID      string
	SessionID      string
	ExecutionMode  ExecutionMode
	ReasonCode     string
	ReasonText     string
	RequestedAt    string
	Status         string
	PublishCount   int
	LastError      string
}

type QueueOutboxRequest struct {
	OutboxID      string
	AttemptID     string
	RunID         string
	NodeRunID     string
	TerminalSeq   int
	TerminalState string
	PayloadJSON   string
	CreatedAt     string
}

type FinalizeTerminalRequest struct {
	OutboxID           string
	AttemptID          string
	RunID              string
	NodeRunID          string
	TerminalState      string
	PayloadJSON        string
	CommittedAt        string
	TerminalReasonCode string
	FailureCode        string
	FailureText        string
	ProviderSessionID  string
}

type FinalizeTerminalResult struct {
	Applied      bool
	AttemptState string
}

type CommitRecoveredTerminalRequest struct {
	AttemptID          string
	RunID              string
	NodeRunID          string
	TerminalState      string
	PayloadJSON        string
	CommittedAt        string
	TerminalReasonCode string
	FailureCode        string
	FailureText        string
	ProviderSessionID  string
}

type UpdateOutboxRequest struct {
	OutboxID      string
	TerminalState string
	PayloadJSON   string
	UpdatedAt     string
}

type ObserveTerminalRequest struct {
	OutboxID          string
	AttemptID         string
	RunID             string
	NodeRunID         string
	TerminalSeq       int
	TerminalState     string
	PayloadJSON       string
	ObservedAt        string
	FailureCode       string
	FailureText       string
	ProviderSessionID string
}

type QueueAttemptCancelRequest struct {
	CancelOutboxID string
	AttemptID      string
	RunID          string
	NodeRunID      string
	SessionID      string
	ReasonCode     string
	ReasonText     string
	RequestedAt    string
	CreatedAt      string
}

func New(db *sql.DB) *Store {
	return &Store{db: db}
}

type EnqueueAttemptRequest struct {
	AttemptID             string
	RunID                 string
	NodeRunID             string
	ExecutionMode         ExecutionMode
	ProviderTimeoutMillis int64
	SessionRef            string
	ProviderSessionID     string
	CreatedAt             string
}

type UpdateProviderSessionIDRequest struct {
	AttemptID         string
	ProviderSessionID string
	UpdatedAt         string
}

func (s *Store) EnqueueAttempt(ctx context.Context, req EnqueueAttemptRequest) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	if strings.TrimSpace(req.AttemptID) == "" || strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.NodeRunID) == "" {
		return fmt.Errorf("attempt_id, run_id, and node_run_id are required")
	}
	createdAt := strings.TrimSpace(req.CreatedAt)
	if createdAt == "" {
		createdAt = time.Now().UTC().Format(time.RFC3339)
	}
	executionMode := NormalizeExecutionMode(string(req.ExecutionMode))
	providerTimeoutMillis := req.ProviderTimeoutMillis
	if providerTimeoutMillis < 0 {
		providerTimeoutMillis = 0
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, provider_timeout_ms, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at)
		 VALUES (?, ?, ?, 'queued', ?, ?, '', NULL, 0, ?, ?, NULL, NULL, NULL, NULL, '', '', ?, ?)`,
		strings.TrimSpace(req.AttemptID),
		strings.TrimSpace(req.RunID),
		strings.TrimSpace(req.NodeRunID),
		string(executionMode),
		providerTimeoutMillis,
		strings.TrimSpace(req.SessionRef),
		strings.TrimSpace(req.ProviderSessionID),
		createdAt,
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("enqueue attempt %q: %w", req.AttemptID, err)
	}
	return nil
}

func (s *Store) ListQueuedAttempts(ctx context.Context, limit int) ([]AttemptExecution, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store database is required")
	}
	if limit <= 0 {
		limit = 1
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT attempt_id, run_id, node_run_id, state, execution_mode, provider_timeout_ms, session_ref, provider_session_id
		 FROM attempt_executions
		 WHERE state = 'queued'
		 ORDER BY created_at ASC, attempt_id ASC
		 LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list queued attempts: %w", err)
	}
	defer rows.Close()

	attempts := make([]AttemptExecution, 0)
	for rows.Next() {
		var rec AttemptExecution
		var executionMode string
		if err := rows.Scan(&rec.AttemptID, &rec.RunID, &rec.NodeRunID, &rec.State, &executionMode, &rec.ProviderTimeoutMillis, &rec.SessionRef, &rec.ProviderSessionID); err != nil {
			return nil, fmt.Errorf("scan queued attempt: %w", err)
		}
		rec.ExecutionMode = NormalizeExecutionMode(executionMode)
		attempts = append(attempts, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queued attempts: %w", err)
	}

	return attempts, nil
}

func (s *Store) GetAttempt(ctx context.Context, attemptID string) (AttemptExecution, bool, error) {
	if s == nil || s.db == nil {
		return AttemptExecution{}, false, fmt.Errorf("store database is required")
	}
	attemptID = strings.TrimSpace(attemptID)
	if attemptID == "" {
		return AttemptExecution{}, false, fmt.Errorf("attempt_id is required")
	}

	var rec AttemptExecution
	var executionMode string
	err := s.db.QueryRowContext(ctx,
		`SELECT attempt_id, run_id, node_run_id, state, execution_mode, provider_timeout_ms, session_ref, provider_session_id,
		        COALESCE(active_provider_invocation_id, ''), COALESCE(canonical_settlement_id, ''),
		        COALESCE(launch_started_at, ''), COALESCE(started_at, ''), COALESCE(terminal_observed_at, ''), COALESCE(committed_at, ''), failure_code, failure_text,
		        COALESCE(progress_event_type, ''), COALESCE(progress_reason_code, ''), COALESCE(progress_at, '')
		 FROM attempt_executions WHERE attempt_id = ?`,
		attemptID,
	).Scan(&rec.AttemptID, &rec.RunID, &rec.NodeRunID, &rec.State, &executionMode, &rec.ProviderTimeoutMillis, &rec.SessionRef, &rec.ProviderSessionID, &rec.ActiveProviderInvocationID, &rec.CanonicalSettlementID, &rec.LaunchStartedAt, &rec.StartedAt, &rec.TerminalObservedAt, &rec.CommittedAt, &rec.FailureCode, &rec.FailureText, &rec.ProgressEventType, &rec.ProgressReasonCode, &rec.ProgressAt)
	if err == sql.ErrNoRows {
		return AttemptExecution{}, false, nil
	}
	if err != nil {
		return AttemptExecution{}, false, fmt.Errorf("query attempt %q: %w", attemptID, err)
	}
	rec.ExecutionMode = NormalizeExecutionMode(executionMode)
	return rec, true, nil
}

func (s *Store) ListRunExecutions(ctx context.Context, runID string) ([]RunExecutionRecord, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store database is required")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("run_id is required")
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT ae.attempt_id,
		        ae.run_id,
		        ae.node_run_id,
		        nr.node_id,
		        nr.attempt,
		        nr.node_status,
		        nr.required_role,
		        nr.agent_id,
		        ae.state,
		        ae.execution_mode,
		        NULLIF(TRIM(ae.session_ref), '') AS logical_session_id,
			        NULLIF(TRIM(ae.provider_session_id), '') AS provider_session_id,
		        ae.created_at AS queued_at,
		        COALESCE(ae.updated_at, ae.started_at, ae.launch_started_at, ae.created_at) AS activity_at,
		        NULLIF(TRIM(ae.progress_event_type), '') AS progress_event_type,
		        NULLIF(TRIM(ae.progress_reason_code), '') AS progress_reason_code,
		        ae.progress_at,
		        ae.launch_started_at,
		        ae.started_at,
		        ae.updated_at,
		        ae.terminal_observed_at,
		        ae.committed_at,
		        NULLIF(TRIM(ae.failure_code), '') AS failure_code,
		        NULLIF(TRIM(ae.failure_text), '') AS failure_text
		 FROM attempt_executions ae
		 JOIN node_runs nr ON nr.node_run_id = ae.node_run_id AND nr.run_id = ae.run_id
		 WHERE ae.run_id = ?
		 ORDER BY activity_at DESC,
		          nr.node_id ASC,
		          nr.attempt DESC,
		          ae.attempt_id ASC`,
		runID,
	)
	if err != nil {
		return nil, fmt.Errorf("list run executions for %q: %w", runID, err)
	}
	defer rows.Close()

	records := make([]RunExecutionRecord, 0)
	for rows.Next() {
		var rec RunExecutionRecord
		var executionMode string
		if err := rows.Scan(
			&rec.AttemptID,
			&rec.RunID,
			&rec.NodeRunID,
			&rec.NodeID,
			&rec.Attempt,
			&rec.NodeStatus,
			&rec.RequiredRole,
			&rec.AgentID,
			&rec.State,
			&executionMode,
			&rec.LogicalSessionID,
			&rec.ProviderSessionID,
			&rec.QueuedAt,
			&rec.ActivityAt,
			&rec.ProgressEventType,
			&rec.ProgressReasonCode,
			&rec.ProgressAt,
			&rec.LaunchStartedAt,
			&rec.StartedAt,
			&rec.UpdatedAt,
			&rec.TerminalObservedAt,
			&rec.CommittedAt,
			&rec.FailureCode,
			&rec.FailureText,
		); err != nil {
			return nil, fmt.Errorf("scan run execution row for %q: %w", runID, err)
		}
		rec.ExecutionMode = NormalizeExecutionMode(executionMode)
		records = append(records, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate run executions for %q: %w", runID, err)
	}

	return records, nil
}

func (s *Store) GetRunExecutionDetail(ctx context.Context, runID, attemptID string) (RunExecutionDetailRecord, bool, error) {
	if s == nil || s.db == nil {
		return RunExecutionDetailRecord{}, false, fmt.Errorf("store database is required")
	}
	runID = strings.TrimSpace(runID)
	attemptID = strings.TrimSpace(attemptID)
	if runID == "" {
		return RunExecutionDetailRecord{}, false, fmt.Errorf("run_id is required")
	}
	if attemptID == "" {
		return RunExecutionDetailRecord{}, false, fmt.Errorf("attempt_id is required")
	}

	var (
		rec                            RunExecutionDetailRecord
		executionMode                  string
		payloadJSON                    string
		activeProviderInvocationID     sql.NullString
		activeProviderInvocationAt     sql.NullString
		canonicalSettlementID          sql.NullString
		canonicalSettlementOutcome     sql.NullString
		canonicalSettlementAt          sql.NullString
		canonicalStructuredPayloadJSON sql.NullString
		canonicalProviderInvocationID  sql.NullString
		canonicalProviderInvocationAt  sql.NullString
		canonicalFinalText             sql.NullString
	)
	err := s.db.QueryRowContext(ctx, `WITH latest_attempt_outbox AS (
		SELECT ao.attempt_id,
		       ao.payload_json,
		       ROW_NUMBER() OVER (
		         PARTITION BY ao.attempt_id
		         ORDER BY ao.terminal_seq DESC, ao.updated_at DESC, ao.outbox_id DESC
		       ) AS outbox_rank
		FROM attempt_outbox ao
		WHERE ao.run_id = ?
		  AND ao.attempt_id = ?
	)
	SELECT ae.attempt_id,
	       ae.run_id,
	       ae.node_run_id,
	       nr.node_id,
	       nr.attempt,
	       nr.node_status,
	       nr.required_role,
	       nr.agent_id,
	       ae.state,
	       ae.execution_mode,
	       NULLIF(TRIM(ae.session_ref), '') AS logical_session_id,
	       NULLIF(TRIM(ae.provider_session_id), '') AS provider_session_id,
	       ae.created_at AS queued_at,
	       COALESCE(ae.updated_at, ae.started_at, ae.launch_started_at, ae.created_at) AS activity_at,
	       NULLIF(TRIM(ae.progress_event_type), '') AS progress_event_type,
	       NULLIF(TRIM(ae.progress_reason_code), '') AS progress_reason_code,
	       ae.progress_at,
	       ae.launch_started_at,
	       ae.started_at,
	       ae.updated_at,
	       ae.terminal_observed_at,
	       ae.committed_at,
	       NULLIF(TRIM(ae.failure_code), '') AS failure_code,
	       NULLIF(TRIM(ae.failure_text), '') AS failure_text,
	       COALESCE(lao.payload_json, '') AS payload_json,
	       NULLIF(TRIM(ae.active_provider_invocation_id), '') AS active_provider_invocation_id,
	       NULLIF(TRIM(api.completion_capability_issued_at), '') AS active_provider_invocation_at,
	       NULLIF(TRIM(ae.canonical_settlement_id), '') AS canonical_settlement_id,
	       NULLIF(TRIM(pcs.outcome), '') AS canonical_settlement_outcome,
	       pcs.settled_at AS canonical_settlement_at,
	       NULLIF(TRIM(pcs.structured_payload_json), '') AS canonical_structured_payload_json,
	       NULLIF(TRIM(pcs.provider_invocation_id), '') AS canonical_provider_invocation_id,
	       NULLIF(TRIM(cpi.completion_capability_issued_at), '') AS canonical_provider_invocation_at,
	       NULLIF(TRIM(pcs.final_text), '') AS canonical_final_text
	FROM attempt_executions ae
	JOIN node_runs nr ON nr.node_run_id = ae.node_run_id AND nr.run_id = ae.run_id
	LEFT JOIN latest_attempt_outbox lao ON lao.attempt_id = ae.attempt_id AND lao.outbox_rank = 1
	LEFT JOIN provider_completion_settlements pcs ON pcs.settlement_id = ae.canonical_settlement_id
	LEFT JOIN provider_invocations api ON api.provider_invocation_id = ae.active_provider_invocation_id
	LEFT JOIN provider_invocations cpi ON cpi.provider_invocation_id = pcs.provider_invocation_id
	WHERE ae.run_id = ?
	  AND ae.attempt_id = ?
	LIMIT 1`, runID, attemptID, runID, attemptID).Scan(
		&rec.AttemptID,
		&rec.RunID,
		&rec.NodeRunID,
		&rec.NodeID,
		&rec.Attempt,
		&rec.NodeStatus,
		&rec.RequiredRole,
		&rec.AgentID,
		&rec.State,
		&executionMode,
		&rec.LogicalSessionID,
		&rec.ProviderSessionID,
		&rec.QueuedAt,
		&rec.ActivityAt,
		&rec.ProgressEventType,
		&rec.ProgressReasonCode,
		&rec.ProgressAt,
		&rec.LaunchStartedAt,
		&rec.StartedAt,
		&rec.UpdatedAt,
		&rec.TerminalObservedAt,
		&rec.CommittedAt,
		&rec.FailureCode,
		&rec.FailureText,
		&payloadJSON,
		&activeProviderInvocationID,
		&activeProviderInvocationAt,
		&canonicalSettlementID,
		&canonicalSettlementOutcome,
		&canonicalSettlementAt,
		&canonicalStructuredPayloadJSON,
		&canonicalProviderInvocationID,
		&canonicalProviderInvocationAt,
		&canonicalFinalText,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return RunExecutionDetailRecord{}, false, nil
		}
		return RunExecutionDetailRecord{}, false, fmt.Errorf("get run execution detail for run=%q attempt=%q: %w", runID, attemptID, err)
	}
	rec.ExecutionMode = NormalizeExecutionMode(executionMode)
	payloadSnapshot, err := decodeRunExecutionPayloadSnapshot(payloadJSON)
	if err != nil {
		return RunExecutionDetailRecord{}, false, fmt.Errorf("decode run execution payload for run=%q attempt=%q: %w", runID, attemptID, err)
	}
	payloadSnapshot = hydrateRunExecutionProjectionSnapshot(rec.ExecutionMode, rec.State, rec.FailureCode, payloadSnapshot, activeProviderInvocationID, activeProviderInvocationAt, canonicalSettlementID, canonicalProviderInvocationID, canonicalProviderInvocationAt, canonicalFinalText)
	rec.ProviderInvocationID = payloadSnapshot.ProviderInvocationID
	rec.ProviderInvocationAt = payloadSnapshot.ProviderInvocationAt
	rec.ObservedOutputText = payloadSnapshot.ObservedOutputText
	rec.ObservedControlVerdict = payloadSnapshot.ObservedControlVerdict
	rec.CanonicalSettlementID = canonicalSettlementID
	rec.CanonicalSettlementOutcome = canonicalSettlementOutcome
	rec.CanonicalSettlementAt = canonicalSettlementAt
	rec.CanonicalStructuredPayloadJSON = canonicalStructuredPayloadJSON
	return rec, true, nil
}

func (s *Store) ListLatestRunExecutionOutputs(ctx context.Context, req ListRunExecutionOutputsRequest) ([]RunExecutionOutputRecord, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store database is required")
	}
	runID := strings.TrimSpace(req.RunID)
	if runID == "" {
		return nil, fmt.Errorf("run_id is required")
	}
	states := compactTrimmedStrings(req.States)
	if len(states) == 0 {
		return nil, fmt.Errorf("at least one state is required")
	}
	nodeClasses := compactTrimmedStrings(req.NodeClasses)

	args := []any{runID}
	query := strings.Builder{}
	query.WriteString(`WITH latest_attempt_outbox AS (
		SELECT ao.attempt_id,
		       ao.payload_json,
		       ROW_NUMBER() OVER (
		         PARTITION BY ao.attempt_id
		         ORDER BY ao.terminal_seq DESC, ao.updated_at DESC, ao.outbox_id DESC
		       ) AS outbox_rank
		FROM attempt_outbox ao
		WHERE ao.run_id = ?
	), ranked_execution_outputs AS (
		SELECT ae.attempt_id,
		       ae.run_id,
		       ae.node_run_id,
		       nr.node_id,
		       COALESCE(wn.node_class, '') AS node_class,
		       ae.execution_mode,
		       nr.attempt,
		       nr.required_role,
		       nr.agent_id,
		       NULLIF(TRIM(a.display_name), '') AS agent_display_name,
		       ae.state,
		       NULLIF(TRIM(ae.failure_code), '') AS failure_code,
		       NULLIF(TRIM(ae.failure_text), '') AS failure_text,
		       NULLIF(TRIM(ae.session_ref), '') AS logical_session_id,
		       NULLIF(TRIM(ae.provider_session_id), '') AS provider_session_id,
		       ae.committed_at,
		       ae.terminal_observed_at,
		       COALESCE(lao.payload_json, '') AS payload_json,
		       NULLIF(TRIM(ae.active_provider_invocation_id), '') AS active_provider_invocation_id,
		       NULLIF(TRIM(api.completion_capability_issued_at), '') AS active_provider_invocation_at,
		       NULLIF(TRIM(ae.canonical_settlement_id), '') AS canonical_settlement_id,
		       NULLIF(TRIM(pcs.provider_invocation_id), '') AS canonical_provider_invocation_id,
		       NULLIF(TRIM(cpi.completion_capability_issued_at), '') AS canonical_provider_invocation_at,
		       NULLIF(TRIM(pcs.final_text), '') AS canonical_final_text,
		       ROW_NUMBER() OVER (
		         PARTITION BY nr.node_id
		         ORDER BY CASE
		           WHEN COALESCE(TRIM(nr.superseded_at), '') = '' THEN 0
		           ELSE 1
		         END ASC,
		                  nr.attempt DESC,
		                  COALESCE(ae.committed_at, ae.terminal_observed_at, ae.updated_at, ae.created_at) DESC,
		                  ae.attempt_id DESC
		       ) AS node_rank
		FROM attempt_executions ae
		JOIN node_runs nr ON nr.node_run_id = ae.node_run_id AND nr.run_id = ae.run_id
		JOIN workflow_runs wr ON wr.run_id = ae.run_id
		LEFT JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		LEFT JOIN agents a ON a.agent_id = nr.agent_id
		LEFT JOIN latest_attempt_outbox lao ON lao.attempt_id = ae.attempt_id AND lao.outbox_rank = 1
		LEFT JOIN provider_completion_settlements pcs ON pcs.settlement_id = ae.canonical_settlement_id
		LEFT JOIN provider_invocations api ON api.provider_invocation_id = ae.active_provider_invocation_id
		LEFT JOIN provider_invocations cpi ON cpi.provider_invocation_id = pcs.provider_invocation_id
		WHERE ae.run_id = ?`)
	args = append(args, runID)
	appendStringFilterClause(&query, &args, "ae.state", states)
	appendStringFilterClause(&query, &args, "wn.node_class", nodeClasses)
	query.WriteString(`
	)
	SELECT attempt_id,
	       run_id,
	       node_run_id,
	       node_id,
	       node_class,
	       execution_mode,
	       attempt,
	       required_role,
	       agent_id,
	       agent_display_name,
	       state,
	       failure_code,
	       failure_text,
	       logical_session_id,
	       provider_session_id,
	       committed_at,
	       terminal_observed_at,
	       payload_json,
	       active_provider_invocation_id,
	       active_provider_invocation_at,
	       canonical_settlement_id,
	       canonical_provider_invocation_id,
	       canonical_provider_invocation_at,
	       canonical_final_text
	FROM ranked_execution_outputs
	WHERE node_rank = 1
	ORDER BY node_id ASC`)

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list latest run execution outputs for %q: %w", runID, err)
	}
	defer rows.Close()

	records := make([]RunExecutionOutputRecord, 0)
	for rows.Next() {
		var rec RunExecutionOutputRecord
		var executionMode string
		var payloadJSON string
		var activeProviderInvocationID sql.NullString
		var activeProviderInvocationAt sql.NullString
		var canonicalSettlementID sql.NullString
		var canonicalProviderInvocationID sql.NullString
		var canonicalProviderInvocationAt sql.NullString
		var canonicalFinalText sql.NullString
		if err := rows.Scan(
			&rec.AttemptID,
			&rec.RunID,
			&rec.NodeRunID,
			&rec.NodeID,
			&rec.NodeClass,
			&executionMode,
			&rec.Attempt,
			&rec.RequiredRole,
			&rec.AgentID,
			&rec.AgentDisplayName,
			&rec.State,
			&rec.FailureCode,
			&rec.FailureText,
			&rec.LogicalSessionID,
			&rec.ProviderSessionID,
			&rec.CommittedAt,
			&rec.TerminalObservedAt,
			&payloadJSON,
			&activeProviderInvocationID,
			&activeProviderInvocationAt,
			&canonicalSettlementID,
			&canonicalProviderInvocationID,
			&canonicalProviderInvocationAt,
			&canonicalFinalText,
		); err != nil {
			return nil, fmt.Errorf("scan latest run execution output for %q: %w", runID, err)
		}
		payloadSnapshot, err := decodeRunExecutionPayloadSnapshot(payloadJSON)
		if err != nil {
			return nil, fmt.Errorf("decode latest run execution payload for run=%q attempt=%q: %w", runID, rec.AttemptID, err)
		}
		payloadSnapshot = hydrateRunExecutionProjectionSnapshot(NormalizeExecutionMode(executionMode), rec.State, rec.FailureCode, payloadSnapshot, activeProviderInvocationID, activeProviderInvocationAt, canonicalSettlementID, canonicalProviderInvocationID, canonicalProviderInvocationAt, canonicalFinalText)
		rec.OutputText = payloadSnapshot.ObservedOutputText
		rec.OutcomeReasonCode = payloadSnapshot.OutcomeReasonCode
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate latest run execution outputs for %q: %w", runID, err)
	}
	return records, nil
}

func decodeRunExecutionPayloadSnapshot(payloadJSON string) (runExecutionPayloadSnapshot, error) {
	trimmed := strings.TrimSpace(payloadJSON)
	if trimmed == "" {
		return runExecutionPayloadSnapshot{}, nil
	}
	var payload struct {
		ProviderInvocationID string `json:"provider_invocation_id"`
		ProviderInvocationAt string `json:"provider_invocation_at"`
		OutputText           string `json:"output_text"`
		Outcome              struct {
			ReasonCode string `json:"reason_code"`
		} `json:"outcome"`
	}
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return runExecutionPayloadSnapshot{}, err
	}
	snapshot := runExecutionPayloadSnapshot{}
	if value := strings.TrimSpace(payload.ProviderInvocationID); value != "" {
		snapshot.ProviderInvocationID = sql.NullString{String: value, Valid: true}
	}
	if value := strings.TrimSpace(payload.ProviderInvocationAt); value != "" {
		snapshot.ProviderInvocationAt = sql.NullString{String: value, Valid: true}
	}
	if value := strings.TrimSpace(payload.OutputText); value != "" {
		snapshot.ObservedOutputText = sql.NullString{String: value, Valid: true}
		if verdict := observedControlVerdict(value); verdict != "" {
			snapshot.ObservedControlVerdict = sql.NullString{String: verdict, Valid: true}
		}
	}
	if value := strings.TrimSpace(payload.Outcome.ReasonCode); value != "" {
		snapshot.OutcomeReasonCode = sql.NullString{String: value, Valid: true}
	}
	return snapshot, nil
}

func hydrateRunExecutionProjectionSnapshot(executionMode ExecutionMode, attemptState string, failureCode sql.NullString, payload runExecutionPayloadSnapshot, activeProviderInvocationID, activeProviderInvocationAt, canonicalSettlementID, canonicalProviderInvocationID, canonicalProviderInvocationAt, canonicalFinalText sql.NullString) runExecutionPayloadSnapshot {
	snapshot := payload
	if executionMode != ExecutionModeProviderBacked {
		return snapshot
	}
	if activeProviderInvocationID.Valid {
		snapshot.ProviderInvocationID = activeProviderInvocationID
	}
	if activeProviderInvocationAt.Valid {
		snapshot.ProviderInvocationAt = activeProviderInvocationAt
	}
	if !canonicalSettlementID.Valid || !isCommittedAttemptExecutionState(attemptState) {
		return snapshot
	}
	if canonicalProviderInvocationID.Valid {
		snapshot.ProviderInvocationID = canonicalProviderInvocationID
	}
	if canonicalProviderInvocationAt.Valid {
		snapshot.ProviderInvocationAt = canonicalProviderInvocationAt
	}
	snapshot.ObservedOutputText = sql.NullString{}
	snapshot.ObservedControlVerdict = sql.NullString{}
	if canonicalFinalText.Valid {
		snapshot.ObservedOutputText = canonicalFinalText
		if verdict := observedControlVerdict(canonicalFinalText.String); verdict != "" {
			snapshot.ObservedControlVerdict = sql.NullString{String: verdict, Valid: true}
		}
	}
	if failureCode.Valid && strings.TrimSpace(failureCode.String) != "" {
		snapshot.OutcomeReasonCode = sql.NullString{String: strings.TrimSpace(failureCode.String), Valid: true}
		return snapshot
	}
	if strings.TrimSpace(attemptState) == "committed_success" {
		snapshot.OutcomeReasonCode = sql.NullString{String: "execution_completed", Valid: true}
	}
	return snapshot
}

func isCommittedAttemptExecutionState(state string) bool {
	switch strings.TrimSpace(state) {
	case "committed_success", "committed_failure", "committed_cancelled":
		return true
	default:
		return false
	}
}

func compactTrimmedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func appendStringFilterClause(query *strings.Builder, args *[]any, column string, values []string) {
	if query == nil || len(values) == 0 {
		return
	}
	query.WriteString(" AND ")
	query.WriteString(column)
	query.WriteString(" IN (")
	for idx, value := range values {
		if idx > 0 {
			query.WriteString(", ")
		}
		query.WriteString("?")
		*args = append(*args, value)
	}
	query.WriteString(")")
}

func observedControlVerdict(outputText string) string {
	return verdict.ExtractNearTop(outputText)
}

func (s *Store) UpdateProviderSessionID(ctx context.Context, req UpdateProviderSessionIDRequest) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	attemptID := strings.TrimSpace(req.AttemptID)
	providerSessionID := strings.TrimSpace(req.ProviderSessionID)
	if attemptID == "" || providerSessionID == "" {
		return fmt.Errorf("attempt_id and provider_session_id are required")
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE attempt_executions
		 SET provider_session_id = ?
		 WHERE attempt_id = ?`,
		providerSessionID,
		attemptID,
	)
	if err != nil {
		return fmt.Errorf("update provider session id for attempt %q: %w", attemptID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read provider session update rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("attempt %q not found", attemptID)
	}
	return nil
}

func (s *Store) NextAttemptSeq(ctx context.Context, attemptID string) (int, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("store database is required")
	}
	attemptID = strings.TrimSpace(attemptID)
	if attemptID == "" {
		return 0, fmt.Errorf("attempt_id is required")
	}

	var seq sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT MAX(seq) FROM attempt_event_log WHERE attempt_id = ?`,
		attemptID,
	).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("query max attempt seq: %w", err)
	}
	if !seq.Valid {
		return 1, nil
	}
	return int(seq.Int64) + 1, nil
}

type AcquireLeaseRequest struct {
	AttemptID    string
	LeaseOwner   string
	LeaseUntil   string
	AcquiredAt   string
	StateOnLease string
}

func (s *Store) AcquireLease(ctx context.Context, req AcquireLeaseRequest) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("store database is required")
	}
	attemptID := strings.TrimSpace(req.AttemptID)
	owner := strings.TrimSpace(req.LeaseOwner)
	leaseUntil := strings.TrimSpace(req.LeaseUntil)
	acquiredAt := strings.TrimSpace(req.AcquiredAt)
	stateOnLease := strings.TrimSpace(req.StateOnLease)
	if attemptID == "" || owner == "" || leaseUntil == "" {
		return false, fmt.Errorf("attempt_id, lease_owner, and lease_until are required")
	}
	if acquiredAt == "" {
		acquiredAt = time.Now().UTC().Format(time.RFC3339)
	}
	if stateOnLease == "" {
		stateOnLease = "running"
	}

	res, err := s.db.ExecContext(ctx,
		`UPDATE attempt_executions
		 SET lease_owner = ?,
		     lease_expires_at = ?,
		     lease_version = lease_version + 1,
		     state = ?,
		     started_at = CASE WHEN started_at IS NULL THEN ? ELSE started_at END,
		     updated_at = ?
		 WHERE attempt_id = ?
		   AND state IN ('queued', 'starting', 'running')
		   AND (
		     lease_owner = '' OR
		     lease_expires_at IS NULL OR
		     lease_expires_at <= ? OR
		     lease_owner = ?
		   )`,
		owner,
		leaseUntil,
		stateOnLease,
		acquiredAt,
		acquiredAt,
		attemptID,
		acquiredAt,
		owner,
	)
	if err != nil {
		return false, fmt.Errorf("acquire lease for attempt %q: %w", attemptID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read acquire lease rows affected: %w", err)
	}
	return rows == 1, nil
}

func (s *Store) ObserveTerminal(ctx context.Context, req ObserveTerminalRequest) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	attemptID := strings.TrimSpace(req.AttemptID)
	runID := strings.TrimSpace(req.RunID)
	nodeRunID := strings.TrimSpace(req.NodeRunID)
	outboxID := strings.TrimSpace(req.OutboxID)
	terminalState := strings.TrimSpace(req.TerminalState)
	payload := strings.TrimSpace(req.PayloadJSON)
	if attemptID == "" || runID == "" || nodeRunID == "" || outboxID == "" {
		return fmt.Errorf("outbox_id, attempt_id, run_id, and node_run_id are required")
	}
	if req.TerminalSeq <= 0 {
		return fmt.Errorf("terminal_seq must be >= 1")
	}
	if terminalState == "" {
		terminalState = "committed_success"
	}
	if payload == "" {
		return fmt.Errorf("payload_json is required")
	}
	observedAt := strings.TrimSpace(req.ObservedAt)
	if observedAt == "" {
		observedAt = time.Now().UTC().Format(time.RFC3339)
	}
	failureCode := strings.TrimSpace(req.FailureCode)
	failureText := strings.TrimSpace(req.FailureText)
	if terminalState == "committed_success" {
		failureCode = ""
		failureText = ""
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin terminal observe transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	res, err := tx.ExecContext(ctx,
		`UPDATE attempt_executions
		 SET state = 'terminal_observed',
		     terminal_observed_at = ?,
		     lease_owner = '',
		     lease_expires_at = NULL,
		     provider_session_id = CASE WHEN TRIM(?) <> '' THEN TRIM(?) ELSE provider_session_id END,
		     failure_code = ?,
		     failure_text = ?,
		     updated_at = ?
		 WHERE attempt_id = ?
		   AND run_id = ?
		   AND node_run_id = ?
		   AND state IN ('queued', 'starting', 'running', 'terminal_observed')`,
		observedAt,
		strings.TrimSpace(req.ProviderSessionID),
		strings.TrimSpace(req.ProviderSessionID),
		failureCode,
		failureText,
		observedAt,
		attemptID,
		runID,
		nodeRunID,
	)
	if err != nil {
		return fmt.Errorf("observe attempt terminal state: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read observed terminal rows affected: %w", err)
	}
	if rows == 0 {
		var existingState string
		err = tx.QueryRowContext(ctx,
			`SELECT state FROM attempt_executions WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?`,
			attemptID,
			runID,
			nodeRunID,
		).Scan(&existingState)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("attempt %q not found", attemptID)
			}
			return fmt.Errorf("load observed attempt state: %w", err)
		}
		if existingState == "committed_success" || existingState == "committed_failure" || existingState == "committed_cancelled" {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit terminal observe no-op transaction: %w", err)
			}
			committed = true
			return nil
		}
		if existingState != "terminal_observed" {
			return fmt.Errorf("attempt %q cannot observe terminal from state %q", attemptID, existingState)
		}
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO attempt_outbox (outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, publish_attempts, last_error, created_at, updated_at, published_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', ?, ?, NULL)
		 ON CONFLICT(outbox_id) DO UPDATE SET
		   terminal_state = excluded.terminal_state,
		   payload_json = excluded.payload_json,
		   updated_at = excluded.updated_at
		 WHERE attempt_outbox.status = 'pending'`,
		outboxID,
		attemptID,
		runID,
		nodeRunID,
		req.TerminalSeq,
		terminalState,
		payload,
		observedAt,
		observedAt,
	)
	if err != nil {
		return fmt.Errorf("queue observed terminal outbox %q: %w", outboxID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit terminal observe transaction: %w", err)
	}
	committed = true
	return nil
}

type AppendAttemptEventRequest struct {
	AttemptEventID string
	AttemptID      string
	RunID          string
	NodeRunID      string
	Seq            int
	EventType      string
	ReasonCode     string
	PayloadJSON    string
	CreatedAt      string
}

func (s *Store) AppendAttemptEvent(ctx context.Context, req AppendAttemptEventRequest) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	if strings.TrimSpace(req.AttemptEventID) == "" || strings.TrimSpace(req.AttemptID) == "" || strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.NodeRunID) == "" {
		return fmt.Errorf("attempt_event_id, attempt_id, run_id, and node_run_id are required")
	}
	if req.Seq <= 0 {
		return fmt.Errorf("seq must be >= 1")
	}
	if strings.TrimSpace(req.EventType) == "" || strings.TrimSpace(req.ReasonCode) == "" || strings.TrimSpace(req.PayloadJSON) == "" {
		return fmt.Errorf("event_type, reason_code, and payload_json are required")
	}
	createdAt := strings.TrimSpace(req.CreatedAt)
	if createdAt == "" {
		createdAt = time.Now().UTC().Format(time.RFC3339)
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO attempt_event_log (attempt_event_id, attempt_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		strings.TrimSpace(req.AttemptEventID),
		strings.TrimSpace(req.AttemptID),
		strings.TrimSpace(req.RunID),
		strings.TrimSpace(req.NodeRunID),
		req.Seq,
		strings.TrimSpace(req.EventType),
		strings.TrimSpace(req.ReasonCode),
		strings.TrimSpace(req.PayloadJSON),
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("append attempt event %q: %w", req.AttemptEventID, err)
	}
	return nil
}

type CommitTerminalRequest struct {
	OutboxID           string
	AttemptID          string
	RunID              string
	NodeRunID          string
	TerminalSeq        int
	TerminalState      string
	PayloadJSON        string
	CommittedAt        string
	TerminalReasonCode string
	FailureCode        string
	FailureText        string
	ProviderSessionID  string
}

func (s *Store) CommitTerminal(ctx context.Context, req CommitTerminalRequest) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	attemptID := strings.TrimSpace(req.AttemptID)
	runID := strings.TrimSpace(req.RunID)
	nodeRunID := strings.TrimSpace(req.NodeRunID)
	terminalState := strings.TrimSpace(req.TerminalState)
	outboxID := strings.TrimSpace(req.OutboxID)
	payload := strings.TrimSpace(req.PayloadJSON)
	if attemptID == "" || runID == "" || nodeRunID == "" || terminalState == "" || outboxID == "" {
		return fmt.Errorf("outbox_id, attempt_id, run_id, node_run_id, and terminal_state are required")
	}
	if payload == "" {
		payload = `{}`
	}
	committedAt := strings.TrimSpace(req.CommittedAt)
	if committedAt == "" {
		committedAt = time.Now().UTC().Format(time.RFC3339)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin terminal commit transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	terminalSeq := req.TerminalSeq
	if terminalSeq <= 0 {
		if err := tx.QueryRowContext(ctx,
			`SELECT COALESCE(MAX(seq), 0) + 1 FROM attempt_event_log WHERE attempt_id = ?`,
			attemptID,
		).Scan(&terminalSeq); err != nil {
			return fmt.Errorf("load terminal attempt seq: %w", err)
		}
	}

	terminalEventType, terminalReasonCode := terminalAttemptEventForState(terminalState, strings.TrimSpace(req.TerminalReasonCode), strings.TrimSpace(req.FailureCode))

	res, err := tx.ExecContext(ctx,
		`UPDATE attempt_executions
		 SET state = ?,
		     terminal_observed_at = ?,
		     committed_at = ?,
		     lease_owner = '',
		     lease_expires_at = NULL,
		     provider_session_id = CASE WHEN TRIM(?) <> '' THEN TRIM(?) ELSE provider_session_id END,
		     failure_code = ?,
		     failure_text = ?,
		     updated_at = ?
		 WHERE attempt_id = ?
		   AND state NOT IN ('committed_success', 'committed_failure', 'committed_cancelled')`,
		terminalState,
		committedAt,
		committedAt,
		strings.TrimSpace(req.ProviderSessionID),
		strings.TrimSpace(req.ProviderSessionID),
		strings.TrimSpace(req.FailureCode),
		strings.TrimSpace(req.FailureText),
		committedAt,
		attemptID,
	)
	if err != nil {
		return fmt.Errorf("update attempt terminal state: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read terminal update rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("attempt %q is already terminal or missing", attemptID)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO attempt_event_log (attempt_event_id, attempt_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		fmt.Sprintf("attempt_event:%s:%d", attemptID, terminalSeq),
		attemptID,
		runID,
		nodeRunID,
		terminalSeq,
		terminalEventType,
		terminalReasonCode,
		payload,
		committedAt,
	)
	if err != nil {
		return fmt.Errorf("insert terminal attempt event: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO attempt_outbox (outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, publish_attempts, last_error, created_at, updated_at, published_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', ?, ?, NULL)`,
		outboxID,
		attemptID,
		runID,
		nodeRunID,
		terminalSeq,
		terminalState,
		payload,
		committedAt,
		committedAt,
	)
	if err != nil {
		return fmt.Errorf("insert attempt outbox record: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit terminal transaction: %w", err)
	}
	committed = true
	return nil
}

func (s *Store) CommitRecoveredTerminal(ctx context.Context, req CommitRecoveredTerminalRequest) (FinalizeTerminalResult, error) {
	result := FinalizeTerminalResult{}
	if s == nil || s.db == nil {
		return result, fmt.Errorf("store database is required")
	}
	attemptID := strings.TrimSpace(req.AttemptID)
	runID := strings.TrimSpace(req.RunID)
	nodeRunID := strings.TrimSpace(req.NodeRunID)
	terminalState := strings.TrimSpace(req.TerminalState)
	payload := strings.TrimSpace(req.PayloadJSON)
	if attemptID == "" || runID == "" || nodeRunID == "" || terminalState == "" {
		return result, fmt.Errorf("attempt_id, run_id, node_run_id, and terminal_state are required")
	}
	if payload == "" {
		payload = `{}`
	}
	committedAt := strings.TrimSpace(req.CommittedAt)
	if committedAt == "" {
		committedAt = time.Now().UTC().Format(time.RFC3339)
	}
	failureCode := strings.TrimSpace(req.FailureCode)
	failureText := strings.TrimSpace(req.FailureText)
	if terminalState == "committed_success" {
		failureCode = ""
		failureText = ""
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin recovered terminal commit transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	var terminalSeq int
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0) + 1 FROM attempt_event_log WHERE attempt_id = ?`,
		attemptID,
	).Scan(&terminalSeq); err != nil {
		return result, fmt.Errorf("load recovered terminal attempt seq: %w", err)
	}

	terminalEventType, terminalReasonCode := terminalAttemptEventForState(terminalState, strings.TrimSpace(req.TerminalReasonCode), failureCode)

	res, err := tx.ExecContext(ctx,
		`UPDATE attempt_executions
		 SET state = ?,
		     terminal_observed_at = ?,
		     committed_at = ?,
		     lease_owner = '',
		     lease_expires_at = NULL,
		     provider_session_id = CASE WHEN TRIM(?) <> '' THEN TRIM(?) ELSE provider_session_id END,
		     failure_code = ?,
		     failure_text = ?,
		     updated_at = ?
		 WHERE attempt_id = ?
		   AND run_id = ?
		   AND node_run_id = ?
		   AND state NOT IN ('committed_success', 'committed_failure', 'committed_cancelled')`,
		terminalState,
		committedAt,
		committedAt,
		strings.TrimSpace(req.ProviderSessionID),
		strings.TrimSpace(req.ProviderSessionID),
		failureCode,
		failureText,
		committedAt,
		attemptID,
		runID,
		nodeRunID,
	)
	if err != nil {
		return result, fmt.Errorf("update recovered attempt terminal state: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return result, fmt.Errorf("read recovered terminal rows affected: %w", err)
	}
	if rows == 0 {
		var existingState string
		err = tx.QueryRowContext(ctx,
			`SELECT state FROM attempt_executions WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?`,
			attemptID,
			runID,
			nodeRunID,
		).Scan(&existingState)
		if err != nil {
			if err == sql.ErrNoRows {
				return result, fmt.Errorf("attempt %q not found", attemptID)
			}
			return result, fmt.Errorf("load recovered attempt state: %w", err)
		}
		if !isTerminalAttemptExecutionState(existingState) {
			return result, fmt.Errorf("attempt %q cannot commit recovered terminal from state %q", attemptID, existingState)
		}
		if err := tx.Commit(); err != nil {
			return result, fmt.Errorf("commit recovered terminal no-op transaction: %w", err)
		}
		committed = true
		result.AttemptState = existingState
		return result, nil
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO attempt_event_log (attempt_event_id, attempt_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(attempt_id, seq) DO UPDATE SET
		   run_id = excluded.run_id,
		   node_run_id = excluded.node_run_id,
		   event_type = excluded.event_type,
		   reason_code = excluded.reason_code,
		   payload_json = excluded.payload_json,
		   created_at = excluded.created_at`,
		fmt.Sprintf("attempt_event:%s:%d", attemptID, terminalSeq),
		attemptID,
		runID,
		nodeRunID,
		terminalSeq,
		terminalEventType,
		terminalReasonCode,
		payload,
		committedAt,
	)
	if err != nil {
		return result, fmt.Errorf("upsert recovered terminal attempt event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit recovered terminal transaction: %w", err)
	}
	committed = true
	result.Applied = true
	result.AttemptState = terminalState
	return result, nil
}

func (s *Store) FinalizeTerminal(ctx context.Context, req FinalizeTerminalRequest) (FinalizeTerminalResult, error) {
	result := FinalizeTerminalResult{}
	if s == nil || s.db == nil {
		return result, fmt.Errorf("store database is required")
	}
	outboxID := strings.TrimSpace(req.OutboxID)
	attemptID := strings.TrimSpace(req.AttemptID)
	runID := strings.TrimSpace(req.RunID)
	nodeRunID := strings.TrimSpace(req.NodeRunID)
	terminalState := strings.TrimSpace(req.TerminalState)
	payload := strings.TrimSpace(req.PayloadJSON)
	if outboxID == "" || attemptID == "" || runID == "" || nodeRunID == "" || terminalState == "" {
		return result, fmt.Errorf("outbox_id, attempt_id, run_id, node_run_id, and terminal_state are required")
	}
	if payload == "" {
		payload = `{}`
	}
	committedAt := strings.TrimSpace(req.CommittedAt)
	if committedAt == "" {
		committedAt = time.Now().UTC().Format(time.RFC3339)
	}
	failureCode := strings.TrimSpace(req.FailureCode)
	failureText := strings.TrimSpace(req.FailureText)
	terminalReasonCode := strings.TrimSpace(req.TerminalReasonCode)
	if terminalState == "committed_success" {
		failureCode = ""
		failureText = ""
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin terminal finalize transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	var terminalSeq int
	err = tx.QueryRowContext(ctx,
		`SELECT terminal_seq FROM attempt_outbox WHERE outbox_id = ? AND status = 'pending'`,
		outboxID,
	).Scan(&terminalSeq)
	if err != nil {
		if err == sql.ErrNoRows {
			return result, fmt.Errorf("outbox record %q not pending", outboxID)
		}
		return result, fmt.Errorf("load terminal outbox seq for %q: %w", outboxID, err)
	}

	terminalEventType, terminalReasonCode := terminalAttemptEventForState(terminalState, terminalReasonCode, failureCode)

	res, err := tx.ExecContext(ctx,
		`UPDATE attempt_executions
		 SET state = ?,
		     terminal_observed_at = ?,
		     committed_at = ?,
		     lease_owner = '',
		     lease_expires_at = NULL,
		     provider_session_id = CASE WHEN TRIM(?) <> '' THEN TRIM(?) ELSE provider_session_id END,
		     failure_code = ?,
		     failure_text = ?,
		     updated_at = ?
		 WHERE attempt_id = ?
		   AND run_id = ?
		   AND node_run_id = ?
		   AND state NOT IN ('committed_success', 'committed_failure', 'committed_cancelled')`,
		terminalState,
		committedAt,
		committedAt,
		strings.TrimSpace(req.ProviderSessionID),
		strings.TrimSpace(req.ProviderSessionID),
		failureCode,
		failureText,
		committedAt,
		attemptID,
		runID,
		nodeRunID,
	)
	if err != nil {
		return result, fmt.Errorf("update finalized attempt terminal state: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return result, fmt.Errorf("read finalized attempt rows affected: %w", err)
	}
	if rows == 0 {
		var existingState string
		err = tx.QueryRowContext(ctx,
			`SELECT state FROM attempt_executions WHERE attempt_id = ?`,
			attemptID,
		).Scan(&existingState)
		if err != nil {
			if err == sql.ErrNoRows {
				return result, fmt.Errorf("attempt %q not found", attemptID)
			}
			return result, fmt.Errorf("load finalized attempt state: %w", err)
		}
		if !isTerminalAttemptExecutionState(existingState) {
			return result, fmt.Errorf("attempt %q cannot finalize from state %q", attemptID, existingState)
		}
		if err := updatePendingOutboxTerminalStateTx(ctx, tx, outboxID, existingState, committedAt); err != nil {
			return result, err
		}
		if err := tx.Commit(); err != nil {
			return result, fmt.Errorf("commit finalized terminal no-op transaction: %w", err)
		}
		committed = true
		result.AttemptState = existingState
		return result, nil
	}
	if err := updatePendingOutboxTerminalStateTx(ctx, tx, outboxID, terminalState, committedAt); err != nil {
		return result, err
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO attempt_event_log (attempt_event_id, attempt_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(attempt_id, seq) DO UPDATE SET
		   run_id = excluded.run_id,
		   node_run_id = excluded.node_run_id,
		   event_type = excluded.event_type,
		   reason_code = excluded.reason_code,
		   payload_json = excluded.payload_json,
		   created_at = excluded.created_at`,
		fmt.Sprintf("attempt_event:%s:%d", attemptID, terminalSeq),
		attemptID,
		runID,
		nodeRunID,
		terminalSeq,
		terminalEventType,
		terminalReasonCode,
		payload,
		committedAt,
	)
	if err != nil {
		return result, fmt.Errorf("upsert finalized terminal attempt event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit finalized terminal transaction: %w", err)
	}
	committed = true
	result.Applied = true
	result.AttemptState = terminalState
	return result, nil
}

func isTerminalAttemptExecutionState(state string) bool {
	switch strings.TrimSpace(state) {
	case "committed_success", "committed_failure", "committed_cancelled":
		return true
	default:
		return false
	}
}

func nullIfEmpty(value string) any {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func updatePendingOutboxTerminalStateTx(ctx context.Context, tx *sql.Tx, outboxID, terminalState, updatedAt string) error {
	res, err := tx.ExecContext(ctx,
		`UPDATE attempt_outbox
		 SET terminal_state = ?,
		     updated_at = ?
		 WHERE outbox_id = ?
		   AND status = 'pending'`,
		strings.TrimSpace(terminalState),
		updatedAt,
		strings.TrimSpace(outboxID),
	)
	if err != nil {
		return fmt.Errorf("update outbox terminal state for %q: %w", outboxID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read outbox terminal update rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("outbox record %q not pending", outboxID)
	}
	return nil
}

func terminalAttemptEventForState(terminalState, terminalReasonCode, failureCode string) (string, string) {
	trimmedState := strings.TrimSpace(terminalState)
	switch trimmedState {
	case "committed_failure":
		return "attempt.failed", firstNonEmptyTrimmed(terminalReasonCode, failureCode, "internal_error")
	case "committed_cancelled":
		return "attempt.cancelled", firstNonEmptyTrimmed(terminalReasonCode, failureCode, "run_terminalized")
	default:
		return "attempt.succeeded", firstNonEmptyTrimmed(terminalReasonCode, "execution_completed")
	}
}

func firstNonEmptyTrimmed(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func (s *Store) ListPendingOutbox(ctx context.Context, limit int) ([]OutboxRecord, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store database is required")
	}
	if limit <= 0 {
		limit = 1
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT ao.outbox_id,
		        ao.attempt_id,
		        ao.run_id,
		        ao.node_run_id,
		        ao.terminal_seq,
		        ao.terminal_state,
		        COALESCE(ae.state, ''),
		        ao.payload_json,
		        ao.status,
		        ao.publish_attempts,
		        COALESCE(ae.failure_code, ''),
		        COALESCE(ae.failure_text, '')
		 FROM attempt_outbox ao
		 LEFT JOIN attempt_executions ae ON ae.attempt_id = ao.attempt_id
		 WHERE ao.status = 'pending'
		 ORDER BY ao.created_at ASC, ao.outbox_id ASC
		 LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list pending outbox records: %w", err)
	}
	defer rows.Close()

	records := make([]OutboxRecord, 0)
	for rows.Next() {
		var rec OutboxRecord
		if err := rows.Scan(&rec.OutboxID, &rec.AttemptID, &rec.RunID, &rec.NodeRunID, &rec.TerminalSeq, &rec.TerminalState, &rec.AttemptState, &rec.PayloadJSON, &rec.Status, &rec.PublishCount, &rec.FailureCode, &rec.FailureText); err != nil {
			return nil, fmt.Errorf("scan pending outbox record: %w", err)
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending outbox records: %w", err)
	}

	return records, nil
}

func (s *Store) ListPendingAttemptCancels(ctx context.Context, limit int) ([]AttemptCancelRecord, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store database is required")
	}
	if limit <= 0 {
		limit = 1
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT aco.cancel_outbox_id, aco.attempt_id, aco.run_id, aco.node_run_id,
		        COALESCE(NULLIF(TRIM(ae.provider_session_id), ''), aco.session_id) AS session_id,
		        COALESCE(ae.execution_mode, 'provider_backed') AS execution_mode,
		        aco.reason_code, aco.reason_text, aco.requested_at, aco.status, aco.publish_attempts, aco.last_error
		 FROM attempt_cancel_outbox aco
		 LEFT JOIN attempt_executions ae ON ae.attempt_id = aco.attempt_id
		 WHERE status = 'pending'
		 ORDER BY aco.created_at ASC, aco.cancel_outbox_id ASC
		 LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list pending attempt cancels: %w", err)
	}
	defer rows.Close()

	records := make([]AttemptCancelRecord, 0)
	for rows.Next() {
		var rec AttemptCancelRecord
		var executionMode string
		if err := rows.Scan(&rec.CancelOutboxID, &rec.AttemptID, &rec.RunID, &rec.NodeRunID, &rec.SessionID, &executionMode, &rec.ReasonCode, &rec.ReasonText, &rec.RequestedAt, &rec.Status, &rec.PublishCount, &rec.LastError); err != nil {
			return nil, fmt.Errorf("scan pending attempt cancel: %w", err)
		}
		rec.ExecutionMode = NormalizeExecutionMode(executionMode)
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending attempt cancels: %w", err)
	}

	return records, nil
}

func (s *Store) MarkOutboxPublished(ctx context.Context, outboxID, publishedAt string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	outboxID = strings.TrimSpace(outboxID)
	publishedAt = strings.TrimSpace(publishedAt)
	if outboxID == "" {
		return fmt.Errorf("outbox_id is required")
	}
	if publishedAt == "" {
		publishedAt = time.Now().UTC().Format(time.RFC3339)
	}

	res, err := s.db.ExecContext(ctx,
		`UPDATE attempt_outbox
		 SET status = 'published',
		     publish_attempts = publish_attempts + 1,
		     last_error = '',
		     updated_at = ?,
		     published_at = ?
		 WHERE outbox_id = ? AND status = 'pending'`,
		publishedAt,
		publishedAt,
		outboxID,
	)
	if err != nil {
		return fmt.Errorf("mark outbox record %q published: %w", outboxID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read outbox publish rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("outbox record %q not pending", outboxID)
	}
	return nil
}

func (s *Store) RecordOutboxFailure(ctx context.Context, outboxID, lastError, updatedAt string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	outboxID = strings.TrimSpace(outboxID)
	lastError = strings.TrimSpace(lastError)
	updatedAt = strings.TrimSpace(updatedAt)
	if outboxID == "" {
		return fmt.Errorf("outbox_id is required")
	}
	if lastError == "" {
		lastError = "attempt outbox publish failed"
	}
	if updatedAt == "" {
		updatedAt = time.Now().UTC().Format(time.RFC3339)
	}

	res, err := s.db.ExecContext(ctx,
		`UPDATE attempt_outbox
		 SET publish_attempts = publish_attempts + 1,
		     last_error = ?,
		     updated_at = ?
		 WHERE outbox_id = ? AND status = 'pending'`,
		lastError,
		updatedAt,
		outboxID,
	)
	if err != nil {
		return fmt.Errorf("record outbox failure for %q: %w", outboxID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read outbox failure rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("outbox record %q not pending", outboxID)
	}
	return nil
}

func (s *Store) MarkOutboxFailed(ctx context.Context, outboxID, lastError, updatedAt string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	outboxID = strings.TrimSpace(outboxID)
	lastError = strings.TrimSpace(lastError)
	updatedAt = strings.TrimSpace(updatedAt)
	if outboxID == "" {
		return fmt.Errorf("outbox_id is required")
	}
	if lastError == "" {
		lastError = "attempt outbox replay payload invalid"
	}
	if updatedAt == "" {
		updatedAt = time.Now().UTC().Format(time.RFC3339)
	}

	res, err := s.db.ExecContext(ctx,
		`UPDATE attempt_outbox
		 SET status = 'failed',
		     publish_attempts = publish_attempts + 1,
		     last_error = ?,
		     updated_at = ?,
		     published_at = NULL
		 WHERE outbox_id = ? AND status = 'pending'`,
		lastError,
		updatedAt,
		outboxID,
	)
	if err != nil {
		return fmt.Errorf("mark outbox record %q failed: %w", outboxID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read outbox failed rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("outbox record %q not pending", outboxID)
	}
	return nil
}

func (s *Store) UpdateOutbox(ctx context.Context, req UpdateOutboxRequest) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	outboxID := strings.TrimSpace(req.OutboxID)
	terminalState := strings.TrimSpace(req.TerminalState)
	payload := strings.TrimSpace(req.PayloadJSON)
	updatedAt := strings.TrimSpace(req.UpdatedAt)
	if outboxID == "" || terminalState == "" {
		return fmt.Errorf("outbox_id and terminal_state are required")
	}
	if payload == "" {
		payload = `{}`
	}
	if updatedAt == "" {
		updatedAt = time.Now().UTC().Format(time.RFC3339)
	}

	res, err := s.db.ExecContext(ctx,
		`UPDATE attempt_outbox
		 SET terminal_state = ?,
		     payload_json = ?,
		     updated_at = ?
		 WHERE outbox_id = ?
		   AND status = 'pending'`,
		terminalState,
		payload,
		updatedAt,
		outboxID,
	)
	if err != nil {
		return fmt.Errorf("update outbox record %q: %w", outboxID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read outbox update rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("outbox record %q not pending", outboxID)
	}
	return nil
}

func (s *Store) MarkAttemptCancelPublished(ctx context.Context, cancelOutboxID, publishedAt string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	cancelOutboxID = strings.TrimSpace(cancelOutboxID)
	publishedAt = strings.TrimSpace(publishedAt)
	if cancelOutboxID == "" {
		return fmt.Errorf("cancel_outbox_id is required")
	}
	if publishedAt == "" {
		publishedAt = time.Now().UTC().Format(time.RFC3339)
	}

	res, err := s.db.ExecContext(ctx,
		`UPDATE attempt_cancel_outbox
		 SET status = 'published',
		     publish_attempts = publish_attempts + 1,
		     last_error = '',
		     updated_at = ?,
		     published_at = ?
		 WHERE cancel_outbox_id = ? AND status = 'pending'`,
		publishedAt,
		publishedAt,
		cancelOutboxID,
	)
	if err != nil {
		return fmt.Errorf("mark attempt cancel outbox %q published: %w", cancelOutboxID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read attempt cancel publish rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("attempt cancel outbox %q not pending", cancelOutboxID)
	}
	return nil
}

func (s *Store) RecordAttemptCancelFailure(ctx context.Context, cancelOutboxID, lastError, updatedAt string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	cancelOutboxID = strings.TrimSpace(cancelOutboxID)
	lastError = strings.TrimSpace(lastError)
	updatedAt = strings.TrimSpace(updatedAt)
	if cancelOutboxID == "" {
		return fmt.Errorf("cancel_outbox_id is required")
	}
	if lastError == "" {
		lastError = "attempt cancel failed"
	}
	if updatedAt == "" {
		updatedAt = time.Now().UTC().Format(time.RFC3339)
	}

	res, err := s.db.ExecContext(ctx,
		`UPDATE attempt_cancel_outbox
		 SET publish_attempts = publish_attempts + 1,
		     last_error = ?,
		     updated_at = ?
		 WHERE cancel_outbox_id = ? AND status = 'pending'`,
		lastError,
		updatedAt,
		cancelOutboxID,
	)
	if err != nil {
		return fmt.Errorf("record attempt cancel failure for %q: %w", cancelOutboxID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read attempt cancel failure rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("attempt cancel outbox %q not pending", cancelOutboxID)
	}
	return nil
}

func (s *Store) QueueOutbox(ctx context.Context, req QueueOutboxRequest) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	outboxID := strings.TrimSpace(req.OutboxID)
	attemptID := strings.TrimSpace(req.AttemptID)
	runID := strings.TrimSpace(req.RunID)
	nodeRunID := strings.TrimSpace(req.NodeRunID)
	terminalState := strings.TrimSpace(req.TerminalState)
	payload := strings.TrimSpace(req.PayloadJSON)
	createdAt := strings.TrimSpace(req.CreatedAt)
	if outboxID == "" || attemptID == "" || runID == "" || nodeRunID == "" {
		return fmt.Errorf("outbox_id, attempt_id, run_id, and node_run_id are required")
	}
	if req.TerminalSeq <= 0 {
		return fmt.Errorf("terminal_seq must be >= 1")
	}
	if terminalState == "" {
		terminalState = "committed_success"
	}
	if payload == "" {
		payload = `{}`
	}
	if createdAt == "" {
		createdAt = time.Now().UTC().Format(time.RFC3339)
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO attempt_outbox (outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, publish_attempts, last_error, created_at, updated_at, published_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', ?, ?, NULL)`,
		outboxID,
		attemptID,
		runID,
		nodeRunID,
		req.TerminalSeq,
		terminalState,
		payload,
		createdAt,
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("queue outbox record %q: %w", outboxID, err)
	}
	return nil
}

func (s *Store) QueueAttemptCancel(ctx context.Context, req QueueAttemptCancelRequest) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	cancelOutboxID := strings.TrimSpace(req.CancelOutboxID)
	attemptID := strings.TrimSpace(req.AttemptID)
	runID := strings.TrimSpace(req.RunID)
	nodeRunID := strings.TrimSpace(req.NodeRunID)
	sessionID := strings.TrimSpace(req.SessionID)
	reasonCode := strings.TrimSpace(req.ReasonCode)
	reasonText := strings.TrimSpace(req.ReasonText)
	requestedAt := strings.TrimSpace(req.RequestedAt)
	createdAt := strings.TrimSpace(req.CreatedAt)
	if cancelOutboxID == "" || attemptID == "" || runID == "" || nodeRunID == "" {
		return fmt.Errorf("cancel_outbox_id, attempt_id, run_id, and node_run_id are required")
	}
	if requestedAt == "" {
		requestedAt = time.Now().UTC().Format(time.RFC3339)
	}
	if createdAt == "" {
		createdAt = requestedAt
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO attempt_cancel_outbox (cancel_outbox_id, attempt_id, run_id, node_run_id, session_id, reason_code, reason_text, requested_at, status, publish_attempts, last_error, created_at, updated_at, published_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', ?, ?, NULL)`,
		cancelOutboxID,
		attemptID,
		runID,
		nodeRunID,
		sessionID,
		reasonCode,
		reasonText,
		requestedAt,
		createdAt,
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("queue attempt cancel outbox %q: %w", cancelOutboxID, err)
	}
	return nil
}
