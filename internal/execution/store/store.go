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
	"errors"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/core/verdict"
	"kingdom/internal/sqlitex"
	"kingdom/internal/timestamporder"
)

type Store struct {
	db  *sql.DB
	now func() time.Time
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

// ProviderCompletionMode is the durable authority for how an execution
// attempt may complete. It is selected before enqueue and never inferred from
// capability presence or provider evidence.
type ProviderCompletionMode string

const (
	ProviderCompletionModeNotApplicable  ProviderCompletionMode = "not_applicable"
	ProviderCompletionModeServerResponse ProviderCompletionMode = "server_response"
)

func ParseProviderCompletionMode(raw string) (ProviderCompletionMode, error) {
	mode := ProviderCompletionMode(strings.TrimSpace(raw))
	switch mode {
	case ProviderCompletionModeNotApplicable, ProviderCompletionModeServerResponse:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid provider completion mode: %q", strings.TrimSpace(raw))
	}
}

func validateAttemptCompletionMode(executionMode ExecutionMode, completionMode ProviderCompletionMode) error {
	switch executionMode {
	case ExecutionModeRuntimeOwned:
		if completionMode != ProviderCompletionModeNotApplicable {
			return fmt.Errorf("runtime-owned attempts require completion_mode %q", ProviderCompletionModeNotApplicable)
		}
	case ExecutionModeProviderBacked:
		if completionMode != ProviderCompletionModeServerResponse {
			return fmt.Errorf("provider-backed attempts require completion_mode %q", ProviderCompletionModeServerResponse)
		}
	default:
		return fmt.Errorf("invalid execution mode: %q", executionMode)
	}
	return nil
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
	CompletionMode             ProviderCompletionMode
	ProviderTimeoutMillis      int64
	LeaseOwner                 string
	LeaseExpiresAt             string
	LeaseVersion               int64
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
	AttemptID                string
	RunID                    string
	NodeRunID                string
	NodeID                   string
	Attempt                  int
	NodeStatus               string
	RequiredRole             string
	AgentID                  string
	State                    string
	ExecutionMode            ExecutionMode
	LogicalSessionID         sql.NullString
	ProviderSessionID        sql.NullString
	QueuedAt                 string
	ActivityAt               string
	ProgressEventType        sql.NullString
	ProgressReasonCode       sql.NullString
	ProgressAt               sql.NullString
	LaunchStartedAt          sql.NullString
	StartedAt                sql.NullString
	UpdatedAt                string
	TerminalObservedAt       sql.NullString
	CommittedAt              sql.NullString
	FailureCode              sql.NullString
	FailureText              sql.NullString
	CancellationPending      bool
	CancellationHandoffState sql.NullString
	StopRetryAvailable       bool
}

// RunAttemptTokenUsage is the aggregate token evidence recorded for one
// physical attempt across its provider invocations.
type RunAttemptTokenUsage struct {
	InputTokens      int64
	OutputTokens     int64
	TotalTokens      int64
	ReasoningTokens  int64
	CacheReadTokens  int64
	CacheWriteTokens int64
}

// RunAttemptRetryRelationship is durable retry lineage involving one physical
// attempt. Direction identifies whether this attempt is the source or target.
type RunAttemptRetryRelationship struct {
	Direction       string  `json:"direction"`
	Phase           string  `json:"phase"`
	Trigger         string  `json:"trigger"`
	ReasonCode      string  `json:"reason_code"`
	SourceAttemptID string  `json:"source_attempt_id"`
	SourceNodeRunID string  `json:"source_node_run_id"`
	TargetAttemptID *string `json:"target_attempt_id"`
	TargetNodeRunID *string `json:"target_node_run_id"`
	RetryIndex      int     `json:"retry_index"`
	RetryMax        int     `json:"retry_max"`
	ScheduledAt     string  `json:"scheduled_at"`
	LastUpdatedAt   string  `json:"last_updated_at"`
}

// RunNodeAttemptHistoryRecord is the canonical enriched history projection for
// one physical node execution.
type RunNodeAttemptHistoryRecord struct {
	RunExecutionRecord
	SupersededAt         sql.NullString
	ProviderInvocationID sql.NullString
	TokenUsage           *RunAttemptTokenUsage
	RetryRelationships   []RunAttemptRetryRelationship
}

type RunExecutionDetailRecord struct {
	RunExecutionRecord
	ProviderInvocationID           sql.NullString
	ProviderInvocationAt           sql.NullString
	ObservedOutputText             sql.NullString
	ObservedOutputTextRef          *providerexecution.ContentReference
	ObservedControlVerdict         sql.NullString
	CanonicalSettlementID          sql.NullString
	CanonicalSettlementOutcome     sql.NullString
	CanonicalSettlementAt          sql.NullString
	CanonicalStructuredPayloadJSON sql.NullString
}

type RunExecutionOutputRecord struct {
	AttemptID        string
	RunID            string
	NodeRunID        string
	NodeID           string
	NodeClass        string
	Attempt          int
	RequiredRole     string
	AgentID          string
	AgentDisplayName sql.NullString
	State            string
	FailureCode      sql.NullString
	FailureText      sql.NullString
	OutputText       sql.NullString
	// OutputText is a display preview when OutputTextRef is non-nil. Consumers
	// assembling canonical context must open/copy the referenced content.
	OutputTextRef      *providerexecution.ContentReference
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
	OutputTextRef          *providerexecution.ContentReference
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
	HandoffState   string
	NodeID         string
	NodeClass      string
	BaseSnapshotID string
	CreatedAt      string
}

type AttemptCancelCursor struct {
	CreatedAt      string
	CancelOutboxID string
}

type RecordAttemptCancelFailureRequest struct {
	CancelOutboxID       string
	ExpectedHandoffState string
	LastError            string
	HandoffState         string
	UpdatedAt            string
}

type ActiveRunAttempt struct {
	AttemptID           string
	NodeID              string
	NodeClass           string
	State               string
	CancellationPending bool
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
	return &Store{db: db, now: time.Now}
}

func (s *Store) currentTime() time.Time {
	if s != nil && s.now != nil {
		return s.now()
	}
	return time.Now()
}

type EnqueueAttemptRequest struct {
	AttemptID             string
	RunID                 string
	NodeRunID             string
	ExecutionMode         ExecutionMode
	CompletionMode        ProviderCompletionMode
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
	completionMode, err := ParseProviderCompletionMode(string(req.CompletionMode))
	if err != nil {
		return fmt.Errorf("validate attempt completion mode: %w", err)
	}
	if err := validateAttemptCompletionMode(executionMode, completionMode); err != nil {
		return err
	}
	providerTimeoutMillis := req.ProviderTimeoutMillis
	if providerTimeoutMillis < 0 {
		providerTimeoutMillis = 0
	}

	result, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, provider_timeout_ms, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at)
		 VALUES (?, ?, ?, 'queued', ?, ?, ?, '', NULL, 0, ?, ?, NULL, NULL, NULL, NULL, '', '', ?, ?)`,
		strings.TrimSpace(req.AttemptID),
		strings.TrimSpace(req.RunID),
		strings.TrimSpace(req.NodeRunID),
		string(executionMode),
		string(completionMode),
		providerTimeoutMillis,
		strings.TrimSpace(req.SessionRef),
		strings.TrimSpace(req.ProviderSessionID),
		createdAt,
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("enqueue attempt %q: %w", req.AttemptID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read enqueue attempt rows affected for %q: %w", req.AttemptID, err)
	}
	if rows == 0 {
		existing, found, err := s.GetAttempt(ctx, req.AttemptID)
		if err != nil {
			return err
		}
		if found && existing.CompletionMode != completionMode {
			return fmt.Errorf("attempt %q completion_mode is immutable: existing=%q requested=%q", req.AttemptID, existing.CompletionMode, completionMode)
		}
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
		`SELECT attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, provider_timeout_ms, session_ref, provider_session_id
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
		var executionMode, completionMode string
		if err := rows.Scan(&rec.AttemptID, &rec.RunID, &rec.NodeRunID, &rec.State, &executionMode, &completionMode, &rec.ProviderTimeoutMillis, &rec.SessionRef, &rec.ProviderSessionID); err != nil {
			return nil, fmt.Errorf("scan queued attempt: %w", err)
		}
		rec.ExecutionMode = NormalizeExecutionMode(executionMode)
		rec.CompletionMode = ProviderCompletionMode(completionMode)
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
	var executionMode, completionMode string
	err := s.db.QueryRowContext(ctx,
		`SELECT attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, provider_timeout_ms,
		        lease_owner, COALESCE(lease_expires_at, ''), lease_version, session_ref, provider_session_id,
		        COALESCE(active_provider_invocation_id, ''), COALESCE(canonical_settlement_id, ''),
		        COALESCE(launch_started_at, ''), COALESCE(started_at, ''), COALESCE(terminal_observed_at, ''), COALESCE(committed_at, ''), failure_code, failure_text,
		        COALESCE(progress_event_type, ''), COALESCE(progress_reason_code, ''), COALESCE(progress_at, '')
		 FROM attempt_executions WHERE attempt_id = ?`,
		attemptID,
	).Scan(&rec.AttemptID, &rec.RunID, &rec.NodeRunID, &rec.State, &executionMode, &completionMode, &rec.ProviderTimeoutMillis, &rec.LeaseOwner, &rec.LeaseExpiresAt, &rec.LeaseVersion, &rec.SessionRef, &rec.ProviderSessionID, &rec.ActiveProviderInvocationID, &rec.CanonicalSettlementID, &rec.LaunchStartedAt, &rec.StartedAt, &rec.TerminalObservedAt, &rec.CommittedAt, &rec.FailureCode, &rec.FailureText, &rec.ProgressEventType, &rec.ProgressReasonCode, &rec.ProgressAt)
	if err == sql.ErrNoRows {
		return AttemptExecution{}, false, nil
	}
	if err != nil {
		return AttemptExecution{}, false, fmt.Errorf("query attempt %q: %w", attemptID, err)
	}
	rec.ExecutionMode = NormalizeExecutionMode(executionMode)
	rec.CompletionMode = ProviderCompletionMode(completionMode)
	return rec, true, nil
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
		rec                                RunExecutionDetailRecord
		executionMode                      string
		payloadJSON                        string
		activeProviderInvocationID         sql.NullString
		activeProviderInvocationAt         sql.NullString
		canonicalSettlementID              sql.NullString
		canonicalSettlementOutcome         sql.NullString
		canonicalSettlementAt              sql.NullString
		canonicalStructuredPayloadJSON     sql.NullString
		canonicalProviderInvocationID      sql.NullString
		canonicalProviderInvocationAt      sql.NullString
		canonicalFinalText                 sql.NullString
		canonicalRefJSON, canonicalPreview string
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
	       EXISTS(SELECT 1 FROM attempt_cancel_outbox aco WHERE aco.attempt_id = ae.attempt_id AND aco.status = 'pending') AS cancellation_pending,
	       (SELECT NULLIF(TRIM(aco.handoff_state), '') FROM attempt_cancel_outbox aco WHERE aco.attempt_id = ae.attempt_id AND aco.status = 'pending') AS cancellation_handoff_state,
	       CASE WHEN nr.node_status = 'blocked' AND ae.state = 'committed_cancelled' AND EXISTS(
	           SELECT 1 FROM attempt_cancel_outbox aco
	           JOIN provider_process_guards ppg ON ppg.attempt_id = aco.attempt_id AND ppg.owner_kind = 'decree_attempt' AND ppg.lifecycle = 'acknowledged'
	           WHERE aco.attempt_id = ae.attempt_id AND aco.status = 'pending'
	             AND aco.reason_code = 'agent_unresponsive'
	             AND aco.handoff_state IN ('awaiting_quiescence', 'cancellation_dispatched', 'stop_retry_dispatched')
	       ) THEN 1 ELSE 0 END AS stop_retry_available,
	       COALESCE(lao.payload_json, '') AS payload_json,
	       NULLIF(TRIM(ae.active_provider_invocation_id), '') AS active_provider_invocation_id,
	       NULLIF(TRIM(api.created_at), '') AS active_provider_invocation_at,
	       NULLIF(TRIM(ae.canonical_settlement_id), '') AS canonical_settlement_id,
	       NULLIF(TRIM(pcs.outcome), '') AS canonical_settlement_outcome,
	       pcs.settled_at AS canonical_settlement_at,
	       NULLIF(TRIM(pcs.structured_payload_json), '') AS canonical_structured_payload_json,
	       NULLIF(TRIM(pcs.provider_invocation_id), '') AS canonical_provider_invocation_id,
	       NULLIF(TRIM(cpi.created_at), '') AS canonical_provider_invocation_at,
	       NULLIF(TRIM(pcs.final_text), '') AS canonical_final_text,
	       COALESCE(pcs.final_text_ref_json,''), COALESCE(pcs.final_text_preview,'')
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
		&rec.CancellationPending,
		&rec.CancellationHandoffState,
		&rec.StopRetryAvailable,
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
		&canonicalRefJSON, &canonicalPreview,
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
	verifiedStructured := canonicalStructuredPayloadJSON.String
	if canonicalSettlementOutcome.String != "success" {
		verifiedStructured = "{}"
	}
	if err := hydrateCanonicalContentReference(&payloadSnapshot, rec.State, canonicalProviderInvocationID.String, canonicalRefJSON, canonicalPreview, verifiedStructured); err != nil {
		return rec, false, err
	}
	rec.ProviderInvocationID = payloadSnapshot.ProviderInvocationID
	rec.ProviderInvocationAt = payloadSnapshot.ProviderInvocationAt
	rec.ObservedOutputText = payloadSnapshot.ObservedOutputText
	rec.ObservedOutputTextRef = payloadSnapshot.OutputTextRef
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
		       NULLIF(TRIM(api.created_at), '') AS active_provider_invocation_at,
		       NULLIF(TRIM(ae.canonical_settlement_id), '') AS canonical_settlement_id,
		       NULLIF(TRIM(pcs.provider_invocation_id), '') AS canonical_provider_invocation_id,
		       NULLIF(TRIM(cpi.created_at), '') AS canonical_provider_invocation_at,
		       NULLIF(TRIM(pcs.final_text), '') AS canonical_final_text,
		       COALESCE(pcs.final_text_ref_json,'') AS canonical_ref_json,
		       COALESCE(pcs.final_text_preview,'') AS canonical_preview,
		       CASE WHEN pcs.outcome='success' THEN COALESCE(pcs.structured_payload_json,'{}') ELSE '{}' END AS canonical_structured,
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
	       canonical_final_text,canonical_ref_json,canonical_preview,canonical_structured
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
		var canonicalRefJSON, canonicalPreview, canonicalStructured string
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
			&canonicalRefJSON, &canonicalPreview, &canonicalStructured,
		); err != nil {
			return nil, fmt.Errorf("scan latest run execution output for %q: %w", runID, err)
		}
		payloadSnapshot, err := decodeRunExecutionPayloadSnapshot(payloadJSON)
		if err != nil {
			return nil, fmt.Errorf("decode latest run execution payload for run=%q attempt=%q: %w", runID, rec.AttemptID, err)
		}
		payloadSnapshot = hydrateRunExecutionProjectionSnapshot(NormalizeExecutionMode(executionMode), rec.State, rec.FailureCode, payloadSnapshot, activeProviderInvocationID, activeProviderInvocationAt, canonicalSettlementID, canonicalProviderInvocationID, canonicalProviderInvocationAt, canonicalFinalText)
		if err := hydrateCanonicalContentReference(&payloadSnapshot, rec.State, canonicalProviderInvocationID.String, canonicalRefJSON, canonicalPreview, canonicalStructured); err != nil {
			return nil, err
		}
		rec.OutputText = payloadSnapshot.ObservedOutputText
		rec.OutputTextRef = payloadSnapshot.OutputTextRef
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
	// Provider outbox payloads are projection envelopes, never output authority.
	snapshot.ObservedOutputText = sql.NullString{}
	snapshot.ObservedControlVerdict = sql.NullString{}
	snapshot.OutcomeReasonCode = sql.NullString{}
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

type ClaimQueuedProviderAttemptRequest struct {
	AttemptID  string
	LeaseOwner string
	LeaseUntil string
	ClaimedAt  string
}

type RenewProviderAttemptLeaseRequest struct {
	AttemptID    string
	LeaseOwner   string
	LeaseVersion int64
	LeaseUntil   string
	RenewedAt    string
}

// ClaimQueuedProviderAttempt fences dispatch ownership without advancing the
// provider lifecycle. In particular, started_at remains provider-observed data.
func (s *Store) ClaimQueuedProviderAttempt(ctx context.Context, req ClaimQueuedProviderAttemptRequest) (AttemptExecution, bool, error) {
	if s == nil || s.db == nil {
		return AttemptExecution{}, false, fmt.Errorf("store database is required")
	}
	attemptID := strings.TrimSpace(req.AttemptID)
	owner := strings.TrimSpace(req.LeaseOwner)
	leaseUntil := strings.TrimSpace(req.LeaseUntil)
	claimedAt := strings.TrimSpace(req.ClaimedAt)
	if attemptID == "" || owner == "" || leaseUntil == "" {
		return AttemptExecution{}, false, fmt.Errorf("attempt_id, lease_owner, and lease_until are required")
	}
	if claimedAt == "" {
		claimedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	claimedTime, err := time.Parse(time.RFC3339Nano, claimedAt)
	if err != nil {
		return AttemptExecution{}, false, fmt.Errorf("parse claimed_at: %w", err)
	}
	leaseUntilTime, err := time.Parse(time.RFC3339Nano, leaseUntil)
	if err != nil {
		return AttemptExecution{}, false, fmt.Errorf("parse lease_until: %w", err)
	}
	if !leaseUntilTime.After(claimedTime) {
		return AttemptExecution{}, false, fmt.Errorf("lease_until must be after claimed_at")
	}
	claimedAt = claimedTime.UTC().Format(time.RFC3339Nano)
	leaseUntil = leaseUntilTime.UTC().Format(time.RFC3339Nano)

	var claim AttemptExecution
	claimed := false
	_, err = sqlitex.Transaction(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		var currentOwner string
		var currentExpiry sql.NullString
		err := tx.QueryRowContext(ctx, `SELECT lease_owner, lease_expires_at
			FROM attempt_executions
			WHERE attempt_id = ? AND state = 'queued' AND execution_mode = 'provider_backed'`, attemptID).Scan(&currentOwner, &currentExpiry)
		if err == sql.ErrNoRows {
			return nil
		}
		if err != nil {
			return fmt.Errorf("load queued provider attempt lease %q: %w", attemptID, err)
		}
		if currentOwner != "" && currentExpiry.Valid {
			expiresAt, parseErr := time.Parse(time.RFC3339Nano, currentExpiry.String)
			if parseErr != nil {
				return fmt.Errorf("parse queued provider attempt lease expiry %q: %w", attemptID, parseErr)
			}
			if expiresAt.After(claimedTime) {
				return nil
			}
		}
		result, err := tx.ExecContext(ctx, `UPDATE attempt_executions
		SET lease_owner = ?,
		    lease_expires_at = ?,
		    lease_version = lease_version + 1,
		    updated_at = ?
		WHERE attempt_id = ?
		  AND state = 'queued'
		  AND execution_mode = 'provider_backed'
		  AND lease_owner = ?
		  AND COALESCE(lease_expires_at, '') = ?`, owner, leaseUntil, claimedAt, attemptID, currentOwner, currentExpiry.String)
		if err != nil {
			return fmt.Errorf("claim queued provider attempt %q: %w", attemptID, err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("read queued provider attempt claim rows affected: %w", err)
		}
		if rows == 0 {
			return nil
		}

		var executionMode, completionMode string
		err = tx.QueryRowContext(ctx, `SELECT attempt_id, run_id, node_run_id, state, execution_mode, completion_mode,
		provider_timeout_ms, lease_owner, COALESCE(lease_expires_at, ''), lease_version,
		session_ref, provider_session_id, COALESCE(launch_started_at, ''), COALESCE(started_at, '')
		FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(
			&claim.AttemptID, &claim.RunID, &claim.NodeRunID, &claim.State, &executionMode, &completionMode,
			&claim.ProviderTimeoutMillis, &claim.LeaseOwner, &claim.LeaseExpiresAt, &claim.LeaseVersion,
			&claim.SessionRef, &claim.ProviderSessionID, &claim.LaunchStartedAt, &claim.StartedAt,
		)
		if err != nil {
			return fmt.Errorf("load queued provider attempt claim %q: %w", attemptID, err)
		}
		claim.ExecutionMode = NormalizeExecutionMode(executionMode)
		claim.CompletionMode = ProviderCompletionMode(completionMode)
		if claim.LeaseOwner == "" || claim.LeaseVersion <= 0 {
			return fmt.Errorf("queued provider attempt %q returned invalid lease lineage", attemptID)
		}
		claimed = true
		return nil
	}, sqlitex.Options{})
	if err != nil {
		return AttemptExecution{}, false, fmt.Errorf("queued provider attempt claim transaction: %w", err)
	}
	return claim, claimed, nil
}

// RenewProviderAttemptLease extends an active provider execution lease without
// changing its generation. A missing, cleared, expired, or replaced lease is
// reported as a lost renewal rather than being recreated.
func (s *Store) RenewProviderAttemptLease(ctx context.Context, req RenewProviderAttemptLeaseRequest) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("store database is required")
	}
	attemptID := strings.TrimSpace(req.AttemptID)
	owner := strings.TrimSpace(req.LeaseOwner)
	leaseUntil := strings.TrimSpace(req.LeaseUntil)
	renewedAt := strings.TrimSpace(req.RenewedAt)
	if attemptID == "" || owner == "" || leaseUntil == "" {
		return false, fmt.Errorf("attempt_id, lease_owner, and lease_until are required")
	}
	if req.LeaseVersion <= 0 {
		return false, fmt.Errorf("lease_version must be positive")
	}
	if renewedAt == "" {
		renewedAt = s.currentTime().UTC().Format(time.RFC3339Nano)
	}
	renewedTime, err := time.Parse(time.RFC3339Nano, renewedAt)
	if err != nil {
		return false, fmt.Errorf("parse renewed_at: %w", err)
	}
	leaseUntilTime, err := time.Parse(time.RFC3339Nano, leaseUntil)
	if err != nil {
		return false, fmt.Errorf("parse lease_until: %w", err)
	}
	if !leaseUntilTime.After(renewedTime) {
		return false, fmt.Errorf("lease_until must be after renewed_at")
	}
	renewedAt = renewedTime.UTC().Format(time.RFC3339Nano)
	leaseUntil = leaseUntilTime.UTC().Format(time.RFC3339Nano)

	renewed := false
	_, err = sqlitex.Transaction(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		var currentExpiry string
		err := tx.QueryRowContext(ctx, `SELECT lease_expires_at
			FROM attempt_executions
			WHERE attempt_id = ?
			  AND execution_mode = 'provider_backed'
			  AND state IN ('starting', 'running')
			  AND lease_owner = ?
			  AND lease_version = ?
			  AND lease_expires_at IS NOT NULL`, attemptID, owner, req.LeaseVersion).Scan(&currentExpiry)
		if err == sql.ErrNoRows {
			return nil
		}
		if err != nil {
			return fmt.Errorf("load provider attempt lease %q for renewal: %w", attemptID, err)
		}
		expiresAt, parseErr := time.Parse(time.RFC3339Nano, currentExpiry)
		if parseErr != nil {
			return fmt.Errorf("parse provider attempt lease expiry %q: %w", attemptID, parseErr)
		}
		if !expiresAt.After(renewedTime) || !leaseUntilTime.After(expiresAt) {
			return nil
		}
		result, err := tx.ExecContext(ctx, `UPDATE attempt_executions
		SET lease_expires_at = ?
		WHERE attempt_id = ?
		  AND execution_mode = 'provider_backed'
		  AND state IN ('starting', 'running')
		  AND lease_owner = ?
		  AND lease_version = ?
		  AND lease_expires_at IS NOT NULL
		  AND lease_expires_at = ?`,
			leaseUntil,
			attemptID,
			owner,
			req.LeaseVersion,
			currentExpiry,
		)
		if err != nil {
			return fmt.Errorf("renew provider attempt lease %q: %w", attemptID, err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("read provider attempt lease renewal rows affected: %w", err)
		}
		renewed = rows == 1
		return nil
	}, sqlitex.Options{})
	if err != nil {
		return false, err
	}
	return renewed, nil
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
		acquiredAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if stateOnLease == "" {
		stateOnLease = "running"
	}

	acquired := false
	_, err := sqlitex.Transaction(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		query := fmt.Sprintf(`UPDATE attempt_executions
			SET lease_owner = ?,
			    lease_expires_at = ?,
			    lease_version = lease_version + 1,
			    state = ?,
			    started_at = CASE WHEN started_at IS NULL THEN ? ELSE started_at END,
			    updated_at = ?
		WHERE attempt_id = ?
		  AND execution_mode = 'provider_backed'
		  AND state IN ('queued', 'starting', 'running')
		  AND (
		    lease_owner = '' OR
		    lease_expires_at IS NULL OR
		    TRIM(COALESCE(lease_expires_at, '')) = '' OR
		    %s OR
		    lease_owner = ?
		  )`, timestamporder.ComparisonSQL("lease_expires_at", "?", "<="))
		res, err := tx.ExecContext(ctx, query,
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
			return fmt.Errorf("acquire lease for attempt %q: %w", attemptID, err)
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("read acquire lease rows affected: %w", err)
		}
		acquired = rows == 1
		return nil
	}, sqlitex.Options{})
	if err != nil {
		return false, err
	}
	return acquired, nil
}

func (s *Store) ObserveTerminal(ctx context.Context, req ObserveTerminalRequest) error {
	_, err := sqlitex.Retry(ctx, func() error { return s.observeTerminalOnce(ctx, req) }, sqlitex.Options{})
	return err
}

func (s *Store) observeTerminalOnce(ctx context.Context, req ObserveTerminalRequest) error {
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

// commitTerminalTx is the transaction-scoped terminal write used by composite
// authority operations. It intentionally contains no transaction lifecycle.
func commitTerminalTx(ctx context.Context, tx *sql.Tx, req CommitTerminalRequest) error {
	attemptID, runID, nodeRunID := strings.TrimSpace(req.AttemptID), strings.TrimSpace(req.RunID), strings.TrimSpace(req.NodeRunID)
	terminalState, outboxID := strings.TrimSpace(req.TerminalState), strings.TrimSpace(req.OutboxID)
	if attemptID == "" || runID == "" || nodeRunID == "" || terminalState == "" || outboxID == "" {
		return fmt.Errorf("outbox_id, attempt_id, run_id, node_run_id, and terminal_state are required")
	}
	payload := strings.TrimSpace(req.PayloadJSON)
	if payload == "" {
		payload = `{}`
	}
	committedAt := strings.TrimSpace(req.CommittedAt)
	if committedAt == "" {
		committedAt = time.Now().UTC().Format(time.RFC3339)
	}
	terminalSeq := req.TerminalSeq
	if terminalSeq <= 0 {
		if err := tx.QueryRowContext(ctx, `SELECT terminal_seq FROM attempt_outbox WHERE outbox_id = ?`, outboxID).Scan(&terminalSeq); err != nil {
			if err != sql.ErrNoRows {
				return fmt.Errorf("load existing terminal outbox sequence: %w", err)
			}
			if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) + 1 FROM attempt_event_log WHERE attempt_id = ?`, attemptID).Scan(&terminalSeq); err != nil {
				return fmt.Errorf("load terminal attempt seq: %w", err)
			}
		}
	}
	failureCode, failureText := strings.TrimSpace(req.FailureCode), strings.TrimSpace(req.FailureText)
	if terminalState == "committed_success" {
		failureCode, failureText = "", ""
	}
	eventType, reasonCode := terminalAttemptEventForState(terminalState, strings.TrimSpace(req.TerminalReasonCode), failureCode)
	res, err := tx.ExecContext(ctx, `UPDATE attempt_executions SET state = ?, terminal_observed_at = ?, committed_at = ?, lease_owner = '', lease_expires_at = NULL, provider_session_id = CASE WHEN TRIM(?) <> '' THEN TRIM(?) ELSE provider_session_id END, failure_code = ?, failure_text = ?, updated_at = ? WHERE attempt_id = ? AND run_id = ? AND node_run_id = ? AND state NOT IN ('committed_success', 'committed_failure', 'committed_cancelled')`, terminalState, committedAt, committedAt, strings.TrimSpace(req.ProviderSessionID), strings.TrimSpace(req.ProviderSessionID), failureCode, failureText, committedAt, attemptID, runID, nodeRunID)
	if err != nil {
		return fmt.Errorf("update attempt terminal state: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read terminal update rows affected: %w", err)
	}
	if rows == 0 {
		var existingState, existingRunID, existingNodeRunID, existingCommittedAt, existingFailureCode, existingFailureText, existingProviderSessionID string
		if err := tx.QueryRowContext(ctx, `SELECT state, run_id, node_run_id, COALESCE(committed_at, ''), failure_code, failure_text, provider_session_id FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&existingState, &existingRunID, &existingNodeRunID, &existingCommittedAt, &existingFailureCode, &existingFailureText, &existingProviderSessionID); err != nil {
			return fmt.Errorf("load terminal attempt state: %w", err)
		}
		if existingState != terminalState || existingRunID != runID || existingNodeRunID != nodeRunID || existingCommittedAt != committedAt || existingFailureCode != failureCode || existingFailureText != failureText || (strings.TrimSpace(req.ProviderSessionID) != "" && existingProviderSessionID != strings.TrimSpace(req.ProviderSessionID)) {
			return providerSettlementError("commit_terminal", "terminal_replay_conflict", "", attemptID, existingState)
		}
		eventID := fmt.Sprintf("attempt_event:%s:%d", attemptID, terminalSeq)
		var existingEventAttemptID, existingEventRunID, existingEventNodeRunID, existingEventType, existingReasonCode, existingEventPayload, existingEventCreatedAt string
		var existingEventSeq int
		if err := tx.QueryRowContext(ctx, `SELECT attempt_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at FROM attempt_event_log WHERE attempt_event_id = ?`, eventID).Scan(&existingEventAttemptID, &existingEventRunID, &existingEventNodeRunID, &existingEventSeq, &existingEventType, &existingReasonCode, &existingEventPayload, &existingEventCreatedAt); err != nil {
			return providerSettlementError("commit_terminal", "terminal_event_replay_conflict", "", attemptID, existingState)
		}
		if existingEventAttemptID != attemptID || existingEventRunID != runID || existingEventNodeRunID != nodeRunID || existingEventSeq != terminalSeq || existingEventType != eventType || existingReasonCode != reasonCode || existingEventPayload != payload || existingEventCreatedAt != committedAt {
			return providerSettlementError("commit_terminal", "terminal_event_replay_conflict", "", attemptID, existingState)
		}
		var existingOutboxAttemptID, existingOutboxRunID, existingOutboxNodeRunID, existingOutboxState, existingOutboxPayload, existingOutboxCreatedAt string
		var existingOutboxSeq int
		if err := tx.QueryRowContext(ctx, `SELECT attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, created_at FROM attempt_outbox WHERE outbox_id = ?`, outboxID).Scan(&existingOutboxAttemptID, &existingOutboxRunID, &existingOutboxNodeRunID, &existingOutboxSeq, &existingOutboxState, &existingOutboxPayload, &existingOutboxCreatedAt); err != nil {
			return providerSettlementError("commit_terminal", "terminal_outbox_replay_conflict", "", attemptID, existingState)
		}
		if existingOutboxAttemptID != attemptID || existingOutboxRunID != runID || existingOutboxNodeRunID != nodeRunID || existingOutboxSeq != terminalSeq || existingOutboxState != terminalState || existingOutboxPayload != payload || existingOutboxCreatedAt != committedAt {
			return providerSettlementError("commit_terminal", "terminal_outbox_replay_conflict", "", attemptID, existingState)
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO attempt_event_log (attempt_event_id, attempt_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, fmt.Sprintf("attempt_event:%s:%d", attemptID, terminalSeq), attemptID, runID, nodeRunID, terminalSeq, eventType, reasonCode, payload, committedAt); err != nil {
		return fmt.Errorf("insert terminal attempt event: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO attempt_outbox (outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, publish_attempts, last_error, created_at, updated_at, published_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', ?, ?, NULL)`, outboxID, attemptID, runID, nodeRunID, terminalSeq, terminalState, payload, committedAt, committedAt); err != nil {
		return fmt.Errorf("insert attempt outbox record: %w", err)
	}
	return nil
}

func (s *Store) CommitRecoveredTerminal(ctx context.Context, req CommitRecoveredTerminalRequest) (FinalizeTerminalResult, error) {
	var result FinalizeTerminalResult
	_, err := sqlitex.Retry(ctx, func() error {
		var retryErr error
		result, retryErr = s.commitRecoveredTerminalOnce(ctx, req)
		return retryErr
	}, sqlitex.Options{})
	return result, err
}

func (s *Store) commitRecoveredTerminalOnce(ctx context.Context, req CommitRecoveredTerminalRequest) (FinalizeTerminalResult, error) {
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
	if err := ensureRecoveredTerminalProviderAuthorityTx(ctx, tx, attemptID, runID, nodeRunID); err != nil {
		return result, err
	}

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

func ensureRecoveredTerminalProviderAuthorityTx(ctx context.Context, tx *sql.Tx, attemptID, runID, nodeRunID string) error {
	var executionMode string
	var activeProviderInvocationID string
	var canonicalSettlementID string
	var canonicalMatches int
	err := tx.QueryRowContext(ctx, `SELECT ae.execution_mode,
		COALESCE(ae.active_provider_invocation_id, ''),
		COALESCE(ae.canonical_settlement_id, ''),
		EXISTS(
			SELECT 1
			FROM provider_completion_settlements pcs
			WHERE pcs.settlement_id = ae.canonical_settlement_id
			  AND pcs.provider_invocation_id = ae.active_provider_invocation_id
			  AND pcs.attempt_id = ae.attempt_id
		)
		FROM attempt_executions ae
		WHERE ae.attempt_id = ? AND ae.run_id = ? AND ae.node_run_id = ?`, attemptID, runID, nodeRunID).Scan(
		&executionMode,
		&activeProviderInvocationID,
		&canonicalSettlementID,
		&canonicalMatches,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load recovered terminal provider authority: %w", err)
	}
	if NormalizeExecutionMode(executionMode) == ExecutionModeProviderBacked && strings.TrimSpace(activeProviderInvocationID) != "" && (strings.TrimSpace(canonicalSettlementID) == "" || canonicalMatches == 0) {
		return providerSettlementError("commit_recovered_terminal", "canonical_settlement_required", activeProviderInvocationID, attemptID, "")
	}
	return nil
}

func (s *Store) FinalizeTerminal(ctx context.Context, req FinalizeTerminalRequest) (FinalizeTerminalResult, error) {
	var result FinalizeTerminalResult
	_, err := sqlitex.Retry(ctx, func() error {
		var retryErr error
		result, retryErr = s.finalizeTerminalOnce(ctx, req)
		return retryErr
	}, sqlitex.Options{})
	return result, err
}

func (s *Store) finalizeTerminalOnce(ctx context.Context, req FinalizeTerminalRequest) (FinalizeTerminalResult, error) {
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

const pendingAttemptCancelStates = `('cancellation_requested', 'stop_retry_requested', 'guardian_evidence_recovery_requested', 'awaiting_workspace_quiescence', 'workspace_restore_failed')`

// PendingAttemptCancelHighWatermark fixes the end of one finite scan cycle.
// New cancellations wait for the next cycle instead of delaying revisits.
func (s *Store) PendingAttemptCancelHighWatermark(ctx context.Context) (AttemptCancelCursor, error) {
	if s == nil || s.db == nil {
		return AttemptCancelCursor{}, fmt.Errorf("store database is required")
	}
	var cursor AttemptCancelCursor
	err := s.db.QueryRowContext(ctx, `SELECT created_at, cancel_outbox_id FROM attempt_cancel_outbox
		WHERE status = 'pending' AND handoff_state IN `+pendingAttemptCancelStates+`
		ORDER BY created_at DESC, cancel_outbox_id DESC LIMIT 1`).Scan(&cursor.CreatedAt, &cursor.CancelOutboxID)
	if errors.Is(err, sql.ErrNoRows) {
		return AttemptCancelCursor{}, nil
	}
	if err != nil {
		return AttemptCancelCursor{}, fmt.Errorf("load pending attempt cancellation high watermark: %w", err)
	}
	return cursor, nil
}

func (s *Store) ListPendingAttemptCancels(ctx context.Context, after, through AttemptCancelCursor, limit int) ([]AttemptCancelRecord, error) {
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
		        aco.reason_code, aco.reason_text, aco.requested_at, aco.status, aco.publish_attempts, aco.last_error, aco.handoff_state,
		        COALESCE(nr.node_id, ''), COALESCE(wn.node_class, ''), COALESCE(ae.base_snapshot_id, ''), aco.created_at
		 FROM attempt_cancel_outbox aco
		 LEFT JOIN attempt_executions ae ON ae.attempt_id = aco.attempt_id
		 LEFT JOIN node_runs nr ON nr.node_run_id = aco.node_run_id AND nr.run_id = aco.run_id
		 LEFT JOIN workflow_runs wr ON wr.run_id = aco.run_id
		 LEFT JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		 WHERE status = 'pending'
		   AND handoff_state IN `+pendingAttemptCancelStates+`
		   AND (aco.created_at, aco.cancel_outbox_id) > (?, ?)
		   AND (aco.created_at, aco.cancel_outbox_id) <= (?, ?)
		 ORDER BY aco.created_at ASC, aco.cancel_outbox_id ASC
		 LIMIT ?`,
		after.CreatedAt, after.CancelOutboxID, through.CreatedAt, through.CancelOutboxID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list pending attempt cancels: %w", err)
	}
	defer rows.Close()

	records := make([]AttemptCancelRecord, 0)
	for rows.Next() {
		var rec AttemptCancelRecord
		var executionMode string
		if err := rows.Scan(&rec.CancelOutboxID, &rec.AttemptID, &rec.RunID, &rec.NodeRunID, &rec.SessionID, &executionMode, &rec.ReasonCode, &rec.ReasonText, &rec.RequestedAt, &rec.Status, &rec.PublishCount, &rec.LastError, &rec.HandoffState, &rec.NodeID, &rec.NodeClass, &rec.BaseSnapshotID, &rec.CreatedAt); err != nil {
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

// ListActiveRunAttempts returns the other attempts whose completion work may
// still read or write a run's shared execution workspace.
func (s *Store) ListActiveRunAttempts(ctx context.Context, runID, excludedAttemptID string) ([]ActiveRunAttempt, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store database is required")
	}
	runID = strings.TrimSpace(runID)
	excludedAttemptID = strings.TrimSpace(excludedAttemptID)
	if runID == "" || excludedAttemptID == "" {
		return nil, fmt.Errorf("run_id and excluded attempt_id are required")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT ae.attempt_id, nr.node_id, wn.node_class, ae.state,
		EXISTS(SELECT 1 FROM attempt_cancel_outbox aco WHERE aco.attempt_id = ae.attempt_id AND aco.status = 'pending')
		FROM attempt_executions ae
		JOIN node_runs nr ON nr.node_run_id = ae.node_run_id AND nr.run_id = ae.run_id
		JOIN workflow_runs wr ON wr.run_id = ae.run_id
		JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		WHERE ae.run_id = ?
		  AND ae.attempt_id <> ?
		  AND (
			  ae.state IN ('starting', 'running', 'terminal_observed')
			  OR EXISTS (SELECT 1 FROM attempt_cancel_outbox aco WHERE aco.attempt_id = ae.attempt_id AND aco.status = 'pending')
			  OR EXISTS (SELECT 1 FROM attempt_outbox ao WHERE ao.attempt_id = ae.attempt_id AND ao.status = 'pending')
		  )
		ORDER BY ae.attempt_id ASC`, runID, excludedAttemptID)
	if err != nil {
		return nil, fmt.Errorf("list active run attempts for %q: %w", runID, err)
	}
	defer rows.Close()
	active := make([]ActiveRunAttempt, 0)
	for rows.Next() {
		var attempt ActiveRunAttempt
		if err := rows.Scan(&attempt.AttemptID, &attempt.NodeID, &attempt.NodeClass, &attempt.State, &attempt.CancellationPending); err != nil {
			return nil, fmt.Errorf("scan active run attempt for %q: %w", runID, err)
		}
		active = append(active, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active run attempts for %q: %w", runID, err)
	}
	return active, nil
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
		     handoff_state = 'released',
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

// RecordAttemptCancelFailure only updates the phase observed by the caller.
// Provider proof or a new operator stop authorization may have superseded it.
func (s *Store) RecordAttemptCancelFailure(ctx context.Context, req RecordAttemptCancelFailureRequest) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("store database is required")
	}
	cancelOutboxID := strings.TrimSpace(req.CancelOutboxID)
	expectedHandoffState := strings.TrimSpace(req.ExpectedHandoffState)
	lastError := strings.TrimSpace(req.LastError)
	handoffState := strings.TrimSpace(req.HandoffState)
	updatedAt := strings.TrimSpace(req.UpdatedAt)
	if cancelOutboxID == "" || expectedHandoffState == "" {
		return false, fmt.Errorf("cancel_outbox_id and expected handoff state are required")
	}
	if lastError == "" {
		lastError = "attempt cancel failed"
	}
	if handoffState != "awaiting_quiescence" && handoffState != "guardian_evidence_recovery_requested" && handoffState != "awaiting_workspace_quiescence" && handoffState != "workspace_restore_failed" {
		return false, fmt.Errorf("invalid pending attempt cancellation handoff state %q", handoffState)
	}
	if updatedAt == "" {
		updatedAt = time.Now().UTC().Format(time.RFC3339)
	}

	res, err := s.db.ExecContext(ctx,
		`UPDATE attempt_cancel_outbox
		 SET publish_attempts = publish_attempts + 1,
		     last_error = ?,
		     handoff_state = ?,
		     updated_at = ?
		 WHERE cancel_outbox_id = ? AND status = 'pending' AND handoff_state = ?
		   AND (handoff_state <> ? OR last_error <> ?)`,
		lastError,
		handoffState,
		updatedAt,
		cancelOutboxID,
		expectedHandoffState,
		handoffState,
		lastError,
	)
	if err != nil {
		return false, fmt.Errorf("record attempt cancel failure for %q: %w", cancelOutboxID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read attempt cancel failure rows affected: %w", err)
	}
	return rows == 1, nil
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
		`INSERT OR IGNORE INTO attempt_cancel_outbox (cancel_outbox_id, attempt_id, run_id, node_run_id, session_id, reason_code, reason_text, requested_at, status, handoff_state, publish_attempts, last_error, created_at, updated_at, published_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 'cancellation_requested', 0, '', ?, ?, NULL)`,
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
