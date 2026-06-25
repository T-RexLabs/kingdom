package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/retryengine"
	"kingdom/internal/core/rolecomm"
	"kingdom/internal/core/schemas"
	coreverdict "kingdom/internal/core/verdict"
	executionstore "kingdom/internal/execution/store"
)

const (
	DefaultReconcileInterval = 30 * time.Second
)

const defaultProgressStaleGrace = 30 * time.Second

type retryPlanner interface {
	Plan(code errorcatalog.Code, retriesAttempted int) (retryengine.PlanResult, error)
}

type missingRetryPlanner struct{}

func (p missingRetryPlanner) Plan(code errorcatalog.Code, retriesAttempted int) (retryengine.PlanResult, error) {
	_ = code
	_ = retriesAttempted
	return retryengine.PlanResult{}, fmt.Errorf("retry planner is required")
}

type ReconcileRequest struct {
	Now                        string
	ReassignCandidateThreshold time.Duration
	StaleHeartbeatThreshold    time.Duration
}

type ReconcileResult struct {
	StaleAttemptsFound int
	AttemptsReassigned int
	AttemptsFailed     int
	SessionsRenewed    int
	SessionsClosed     int
	AgentsBlocked      int
}

type Reconciler struct {
	db             *sql.DB
	transition     *TransitionService
	eventIngestor  *AttemptEventIngestor
	registry       *AgentRegistry
	sessionManager *SessionManager
	retryPlanner   retryPlanner
	now            func() time.Time
}

func NewReconciler(db *sql.DB, sessionManager *SessionManager, planner retryPlanner, eventSchemas *schemas.Set) *Reconciler {
	return NewReconcilerWithRoleCommunicationPolicy(db, sessionManager, planner, rolecomm.Policy{}, eventSchemas)
}

func NewReconcilerWithRoleCommunicationPolicy(db *sql.DB, sessionManager *SessionManager, planner retryPlanner, policy rolecomm.Policy, eventSchemas *schemas.Set) *Reconciler {
	if sessionManager == nil {
		sessionManager = NewSessionManager(db, NewSessionLifecycleMetrics())
	}
	if planner == nil {
		planner = missingRetryPlanner{}
	}

	return &Reconciler{
		db:             db,
		transition:     NewTransitionService(db, eventSchemas).WithRoleCommunicationPolicy(policy),
		eventIngestor:  NewAttemptEventIngestor(db, eventSchemas).WithRoleCommunicationPolicy(policy),
		registry:       NewAgentRegistry(db),
		sessionManager: sessionManager,
		retryPlanner:   planner,
		now:            time.Now,
	}
}

func (r *Reconciler) RunStartup(ctx context.Context) (ReconcileResult, error) {
	return r.Reconcile(ctx, ReconcileRequest{Now: r.now().UTC().Format(time.RFC3339)})
}

func (r *Reconciler) Reconcile(ctx context.Context, req ReconcileRequest) (ReconcileResult, error) {
	nowTime, reassignThreshold, staleHeartbeatThreshold, err := normalizeReconcileInputs(req, r.now)
	if err != nil {
		return ReconcileResult{}, err
	}

	if err := r.convergeProviderSettledAttempts(ctx, nowTime); err != nil {
		return ReconcileResult{}, err
	}

	staleAttempts, err := r.findStaleAttempts(ctx, nowTime, reassignThreshold)
	if err != nil {
		return ReconcileResult{}, err
	}

	result := ReconcileResult{StaleAttemptsFound: len(staleAttempts)}
	blockedAgents := make(map[string]struct{}, len(staleAttempts))

	for _, attempt := range staleAttempts {
		recovery, handleErr := r.recoverAttempt(ctx, attempt, nowTime)
		if handleErr != nil {
			return ReconcileResult{}, handleErr
		}

		if recovery.agentBlocked {
			if _, seen := blockedAgents[attempt.AgentID]; !seen {
				result.AgentsBlocked++
			}
			blockedAgents[attempt.AgentID] = struct{}{}
		}
		if recovery.reassigned {
			result.AttemptsReassigned++
		}
		if recovery.failed {
			result.AttemptsFailed++
		}
	}

	if err := r.finalizeDurableTerminalProviderSettlements(ctx, nowTime); err != nil {
		return ReconcileResult{}, err
	}

	if err := r.projectDurableTerminalNodes(ctx, nowTime); err != nil {
		return ReconcileResult{}, err
	}

	staleLeases, err := r.sessionManager.FindStaleLeases(ctx, FindStaleSessionLeasesRequest{
		Now:                     nowTime.UTC().Format(time.RFC3339),
		StaleHeartbeatThreshold: staleHeartbeatThreshold,
	})
	if err != nil {
		return ReconcileResult{}, err
	}

	for _, lease := range staleLeases {
		if _, blocked := blockedAgents[lease.AgentID]; blocked {
			_, closeErr := r.sessionManager.CloseLease(ctx, CloseSessionLeaseRequest{
				SessionID: lease.SessionID,
				UpdatedAt: nowTime.UTC().Format(time.RFC3339),
			})
			if closeErr != nil {
				var stateErr *SessionLeaseStateConflictError
				if !errors.As(closeErr, &stateErr) {
					return ReconcileResult{}, closeErr
				}
			} else {
				result.SessionsClosed++
			}
			continue
		}

		attemptState, launchStartedAt, startedAt, foundAttempt, stateErr := r.sessionLeaseAttemptState(ctx, lease.SessionID)
		if stateErr != nil {
			return ReconcileResult{}, stateErr
		}
		if !foundAttempt || !executionstore.AttemptRetainsSessionLease(attemptState, launchStartedAt, startedAt) {
			_, closeErr := r.sessionManager.CloseLease(ctx, CloseSessionLeaseRequest{
				SessionID: lease.SessionID,
				UpdatedAt: nowTime.UTC().Format(time.RFC3339),
			})
			if closeErr != nil {
				var stateErr *SessionLeaseStateConflictError
				if !errors.As(closeErr, &stateErr) {
					return ReconcileResult{}, closeErr
				}
			} else {
				result.SessionsClosed++
			}
			continue
		}
		if !executionstore.AttemptRenewsSessionLease(attemptState, launchStartedAt, startedAt) {
			continue
		}

		_, renewErr := r.sessionManager.RenewLease(ctx, RenewSessionLeaseRequest{
			SessionID:       lease.SessionID,
			LeaseExpiresAt:  nowTime.Add(staleHeartbeatThreshold).UTC().Format(time.RFC3339),
			LastHeartbeatAt: nowTime.UTC().Format(time.RFC3339),
			UpdatedAt:       nowTime.UTC().Format(time.RFC3339),
		})
		if renewErr != nil {
			var stateErr *SessionLeaseStateConflictError
			if !errors.As(renewErr, &stateErr) {
				return ReconcileResult{}, renewErr
			}
			continue
		}
		result.SessionsRenewed++
	}

	return result, nil
}

func (r *Reconciler) projectDurableTerminalNodes(ctx context.Context, nowTime time.Time) error {
	if r == nil || r.db == nil || r.transition == nil {
		return nil
	}
	rows, err := r.db.QueryContext(ctx, `WITH latest_attempts AS (
		SELECT nr.run_id,
		       nr.node_id,
		       nr.node_run_id,
		       ae.attempt_id,
		       nr.node_status,
		       ae.state,
		       ROW_NUMBER() OVER (
		         PARTITION BY nr.run_id, nr.node_id
		         ORDER BY CASE
		           WHEN COALESCE(TRIM(nr.superseded_at), '') = '' THEN 0
		           ELSE 1
		         END ASC,
		         nr.attempt DESC,
		         nr.node_run_id DESC
		       ) AS rn
		FROM node_runs nr
		JOIN attempt_executions ae ON ae.run_id = nr.run_id AND ae.node_run_id = nr.node_run_id
		WHERE ae.state IN ('committed_success', 'committed_failure', 'committed_cancelled')
	)
	SELECT run_id, node_run_id, attempt_id
	FROM latest_attempts
	WHERE rn = 1
	  AND ((state = 'committed_success' AND node_status <> 'succeeded')
	    OR (state = 'committed_failure' AND node_status <> 'failed')
	    OR (state = 'committed_cancelled' AND node_status <> 'cancelled'))
	ORDER BY run_id ASC, node_run_id ASC`)
	if err != nil {
		return fmt.Errorf("query durable terminal node projections: %w", err)
	}
	defer rows.Close()

	type projectionTarget struct {
		RunID     string
		NodeRunID string
		AttemptID string
	}
	targets := make([]projectionTarget, 0)
	for rows.Next() {
		var target projectionTarget
		if err := rows.Scan(&target.RunID, &target.NodeRunID, &target.AttemptID); err != nil {
			return fmt.Errorf("scan durable terminal node projection target: %w", err)
		}
		targets = append(targets, target)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate durable terminal node projection targets: %w", err)
	}

	occurredAt := nowTime.UTC().Format(time.RFC3339)
	for _, target := range targets {
		if _, err := r.transition.ProjectNodeTerminalFromDurableAttempt(ctx, DurableAttemptNodeProjectionRequest{
			RunID:      target.RunID,
			NodeRunID:  target.NodeRunID,
			AttemptID:  target.AttemptID,
			ActorID:    "runtime_orchestrator",
			OccurredAt: occurredAt,
		}); err != nil {
			return fmt.Errorf("project durable terminal node run=%q node_run=%q: %w", target.RunID, target.NodeRunID, err)
		}
	}
	return nil
}

func (r *Reconciler) finalizeDurableTerminalProviderSettlements(ctx context.Context, nowTime time.Time) error {
	if r == nil || r.db == nil {
		return nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT ae.attempt_id,
	       ae.run_id,
	       ae.node_run_id,
	       ae.state,
	       ae.execution_mode,
	       ae.provider_timeout_ms,
	       ae.session_ref,
	       ae.provider_session_id,
	       COALESCE(ae.active_provider_invocation_id, ''),
	       COALESCE(ae.canonical_settlement_id, ''),
	       COALESCE(ae.terminal_observed_at, ''),
	       COALESCE(ae.committed_at, ''),
	       COALESCE(ae.failure_code, ''),
	       COALESCE(ae.failure_text, '')
	FROM attempt_executions ae
	LEFT JOIN provider_completion_settlements pcs ON pcs.provider_invocation_id = ae.active_provider_invocation_id
	WHERE ae.execution_mode = 'provider_backed'
	  AND ae.state IN ('committed_success', 'committed_failure', 'committed_cancelled')
	  AND COALESCE(TRIM(ae.active_provider_invocation_id), '') <> ''
	  AND (
		  COALESCE(TRIM(ae.canonical_settlement_id), '') = ''
		  OR pcs.settlement_id IS NULL
		  OR COALESCE(TRIM(ae.canonical_settlement_id), '') <> COALESCE(TRIM(pcs.settlement_id), '')
		  OR COALESCE(TRIM(pcs.outcome), '') <> CASE ae.state
			WHEN 'committed_success' THEN 'success'
			WHEN 'committed_failure' THEN 'failed'
			ELSE 'cancelled'
		  END
		  OR COALESCE(TRIM(pcs.failure_reason_code), '') <> CASE
			WHEN ae.state = 'committed_success' THEN ''
			ELSE COALESCE(TRIM(ae.failure_code), '')
		  END
		  OR COALESCE(TRIM(pcs.failure_reason_text), '') <> CASE
			WHEN ae.state = 'committed_success' THEN ''
			ELSE COALESCE(TRIM(ae.failure_text), '')
		  END
	  )
	ORDER BY COALESCE(ae.committed_at, ae.terminal_observed_at, ae.updated_at, ae.created_at) ASC,
	         ae.attempt_id ASC`)
	if err != nil {
		return fmt.Errorf("query durable terminal provider settlements: %w", err)
	}
	defer rows.Close()

	attempts := make([]executionstore.AttemptExecution, 0)
	for rows.Next() {
		var attempt executionstore.AttemptExecution
		var executionMode string
		if err := rows.Scan(
			&attempt.AttemptID,
			&attempt.RunID,
			&attempt.NodeRunID,
			&attempt.State,
			&executionMode,
			&attempt.ProviderTimeoutMillis,
			&attempt.SessionRef,
			&attempt.ProviderSessionID,
			&attempt.ActiveProviderInvocationID,
			&attempt.CanonicalSettlementID,
			&attempt.TerminalObservedAt,
			&attempt.CommittedAt,
			&attempt.FailureCode,
			&attempt.FailureText,
		); err != nil {
			return fmt.Errorf("scan durable terminal provider settlement attempt: %w", err)
		}
		attempt.ExecutionMode = executionstore.NormalizeExecutionMode(executionMode)
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate durable terminal provider settlement attempts: %w", err)
	}

	for _, attempt := range attempts {
		if err := r.finalizeDurableTerminalProviderSettlement(ctx, attempt, nowTime); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) finalizeDurableTerminalProviderSettlement(ctx context.Context, attempt executionstore.AttemptExecution, nowTime time.Time) error {
	if r == nil || r.db == nil {
		return nil
	}
	store := executionstore.New(r.db)
	if err := r.reconcileDurableAttemptProviderSettlement(ctx, store, attempt, nowTime); err != nil {
		return fmt.Errorf("reconcile provider settlement for durable attempt %q: %w", attempt.AttemptID, err)
	}
	return nil
}

func (r *Reconciler) reconcileDurableAttemptProviderSettlement(ctx context.Context, store *executionstore.Store, attempt executionstore.AttemptExecution, nowTime time.Time) error {
	if store == nil || attempt.ExecutionMode != executionstore.ExecutionModeProviderBacked || strings.TrimSpace(attempt.ActiveProviderInvocationID) == "" {
		return nil
	}
	outcome, ok := providerSettlementOutcomeForAttemptState(attempt.State)
	if !ok {
		return nil
	}
	_, found, err := store.GetProviderCompletionSettlementByInvocation(ctx, attempt.ActiveProviderInvocationID)
	if err != nil {
		return fmt.Errorf("load provider settlement for stale attempt %q: %w", attempt.AttemptID, err)
	}
	if found {
		_, err := r.reconcileProviderCompletionSettlementWithRetry(ctx, store, executionstore.ReconcileProviderCompletionSettlementRequest{
			ProviderInvocationID: attempt.ActiveProviderInvocationID,
			AttemptID:            attempt.AttemptID,
			RunID:                attempt.RunID,
			NodeRunID:            attempt.NodeRunID,
			Outcome:              outcome,
			FailureReasonCode:    strings.TrimSpace(attempt.FailureCode),
			FailureReasonText:    strings.TrimSpace(attempt.FailureText),
			SettledAt:            settledAtForDurableAttempt(attempt, nowTime),
		})
		if err != nil {
			return err
		}
		return nil
	}

	settledAt := settledAtForDurableAttempt(attempt, nowTime)
	primaryEvidenceID, err := r.primaryEvidenceForDurableAttemptSettlement(ctx, store, attempt, outcome, settledAt)
	if err != nil {
		return err
	}
	if _, err := r.finalizeProviderCompletionSettlementWithRetry(ctx, store, executionstore.FinalizeProviderCompletionSettlementRequest{
		SettlementID:          executionstore.ProviderCompletionSettlementID(attempt.ActiveProviderInvocationID),
		ProviderInvocationID:  attempt.ActiveProviderInvocationID,
		AttemptID:             attempt.AttemptID,
		RunID:                 attempt.RunID,
		NodeRunID:             attempt.NodeRunID,
		Outcome:               outcome,
		PrimaryEvidenceID:     primaryEvidenceID,
		FailureReasonCode:     strings.TrimSpace(attempt.FailureCode),
		FailureReasonText:     strings.TrimSpace(attempt.FailureText),
		StructuredPayloadJSON: "{}",
		SettledAt:             settledAt,
	}); err != nil {
		return fmt.Errorf("finalize provider settlement for attempt %q: %w", attempt.AttemptID, err)
	}
	return nil
}

func (r *Reconciler) finalizeProviderCompletionSettlementWithRetry(ctx context.Context, store *executionstore.Store, req executionstore.FinalizeProviderCompletionSettlementRequest) (executionstore.ProviderCompletionSettlement, error) {
	return finalizeProviderCompletionSettlementWithRetry(ctx, store, req)
}

func (r *Reconciler) reconcileProviderCompletionSettlementWithRetry(ctx context.Context, store *executionstore.Store, req executionstore.ReconcileProviderCompletionSettlementRequest) (executionstore.ProviderCompletionSettlement, error) {
	return reconcileProviderCompletionSettlementWithRetry(ctx, store, req)
}

func finalizeProviderCompletionSettlementWithRetry(ctx context.Context, store *executionstore.Store, req executionstore.FinalizeProviderCompletionSettlementRequest) (executionstore.ProviderCompletionSettlement, error) {
	var settlement executionstore.ProviderCompletionSettlement
	err := retrySQLiteBusy(ctx, func() error {
		result, err := store.FinalizeProviderCompletionSettlement(ctx, req)
		if err == nil {
			settlement = result
		}
		return err
	})
	return settlement, err
}

func reconcileProviderCompletionSettlementWithRetry(ctx context.Context, store *executionstore.Store, req executionstore.ReconcileProviderCompletionSettlementRequest) (executionstore.ProviderCompletionSettlement, error) {
	var settlement executionstore.ProviderCompletionSettlement
	err := retrySQLiteBusy(ctx, func() error {
		result, err := store.ReconcileProviderCompletionSettlement(ctx, req)
		if err == nil {
			settlement = result
		}
		return err
	})
	return settlement, err
}

func ensureTerminalProviderCompletionEvidenceWithRetry(ctx context.Context, store *executionstore.Store, req executionstore.EnsureTerminalProviderCompletionEvidenceRequest) (executionstore.ProviderCompletionEvidence, error) {
	var evidence executionstore.ProviderCompletionEvidence
	err := retrySQLiteBusy(ctx, func() error {
		result, err := store.EnsureTerminalProviderCompletionEvidence(ctx, req)
		if err == nil {
			evidence = result
		}
		return err
	})
	return evidence, err
}

func providerSettlementOutcomeForAttemptState(state string) (executionstore.ProviderCompletionOutcome, bool) {
	switch strings.TrimSpace(state) {
	case "committed_success":
		return executionstore.ProviderCompletionOutcomeSuccess, true
	case "committed_failure":
		return executionstore.ProviderCompletionOutcomeFailed, true
	case "committed_cancelled":
		return executionstore.ProviderCompletionOutcomeCancelled, true
	default:
		return "", false
	}
}

func settledAtForDurableAttempt(attempt executionstore.AttemptExecution, nowTime time.Time) string {
	settledAt := strings.TrimSpace(attempt.CommittedAt)
	if settledAt == "" {
		settledAt = strings.TrimSpace(attempt.TerminalObservedAt)
	}
	if settledAt == "" {
		settledAt = nowTime.UTC().Format(time.RFC3339)
	}
	return settledAt
}

func (r *Reconciler) primaryEvidenceForDurableAttemptSettlement(ctx context.Context, store *executionstore.Store, attempt executionstore.AttemptExecution, outcome executionstore.ProviderCompletionOutcome, settledAt string) (string, error) {
	evidence, found, err := latestCompletionReceiptEvidence(ctx, store, attempt.ActiveProviderInvocationID)
	if err != nil {
		return "", fmt.Errorf("load receipt evidence for durable attempt %q: %w", attempt.AttemptID, err)
	}
	if found {
		return evidence.EvidenceID, nil
	}
	if outcome == executionstore.ProviderCompletionOutcomeSuccess {
		return "", fmt.Errorf("durable provider success for attempt %q is missing accepted receipt evidence", attempt.AttemptID)
	}
	evidence, err = ensureTerminalProviderCompletionEvidenceWithRetry(ctx, store, executionstore.EnsureTerminalProviderCompletionEvidenceRequest{
		ProviderInvocationID: attempt.ActiveProviderInvocationID,
		AttemptID:            attempt.AttemptID,
		RunID:                attempt.RunID,
		NodeRunID:            attempt.NodeRunID,
		State:                providerSettlementDiagnosticState(attempt.State),
		ReasonCode:           attempt.FailureCode,
		FailureText:          attempt.FailureText,
		EvidenceSource:       "stream",
		CreatedAt:            settledAt,
	})
	if err != nil {
		return "", fmt.Errorf("append provider settlement evidence for attempt %q: %w", attempt.AttemptID, err)
	}
	return evidence.EvidenceID, nil
}

func latestCompletionReceiptEvidence(ctx context.Context, store *executionstore.Store, providerInvocationID string) (executionstore.ProviderCompletionEvidence, bool, error) {
	if store == nil || strings.TrimSpace(providerInvocationID) == "" {
		return executionstore.ProviderCompletionEvidence{}, false, nil
	}
	evidence, err := store.ListProviderCompletionEvidence(ctx, providerInvocationID)
	if err != nil {
		return executionstore.ProviderCompletionEvidence{}, false, err
	}
	for idx := len(evidence) - 1; idx >= 0; idx-- {
		if evidence[idx].EvidenceKind == executionstore.ProviderCompletionEvidenceKindCompletionReceipt {
			return evidence[idx], true, nil
		}
	}
	return executionstore.ProviderCompletionEvidence{}, false, nil
}

func providerSettlementDiagnosticState(attemptState string) string {
	switch strings.TrimSpace(attemptState) {
	case "committed_cancelled":
		return "cancelled"
	default:
		return "failed"
	}
}

func (r *Reconciler) sessionLeaseAttemptState(ctx context.Context, sessionID string) (string, string, string, bool, error) {
	if r == nil || r.db == nil || strings.TrimSpace(sessionID) == "" {
		return "", "", "", false, nil
	}
	var state sql.NullString
	var launchStartedAt sql.NullString
	var startedAt sql.NullString
	err := r.db.QueryRowContext(ctx, `
SELECT state,
	   launch_started_at,
       started_at
FROM attempt_executions
WHERE session_ref = ?
ORDER BY CASE state
	WHEN 'running' THEN 0
	WHEN 'starting' THEN 1
	WHEN 'queued' THEN 2
	ELSE 3
END,
updated_at DESC,
attempt_id DESC
LIMIT 1`, strings.TrimSpace(sessionID)).Scan(&state, &launchStartedAt, &startedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", "", false, nil
	}
	if err != nil {
		return "", "", "", false, fmt.Errorf("load session lease attempt state: %w", err)
	}
	return strings.TrimSpace(state.String), strings.TrimSpace(launchStartedAt.String), strings.TrimSpace(startedAt.String), true, nil
}

type staleAttempt struct {
	NodeRunID     string
	RunID         string
	NodeID        string
	Attempt       int
	Wave          int
	RequiredRole  string
	AgentID       string
	SessionRef    string
	AttemptState  string
	CurrentStatus string
	StartedAt     string
}

type recoveryOutcome struct {
	reassigned   bool
	failed       bool
	agentBlocked bool
}

type staleAttemptQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type providerSettlementRecoveryCandidate struct {
	AttemptID string
	NodeID    string
}

const staleAttemptFailureReasonText = "agent became unresponsive before completion could be confirmed"

func (r *Reconciler) recoverAttempt(ctx context.Context, attempt staleAttempt, nowTime time.Time) (recoveryOutcome, error) {
	plan, err := r.retryPlanner.Plan(errorcatalog.CodeAgentUnresponsive, maxInt(0, attempt.Attempt-1))
	if err != nil {
		return recoveryOutcome{}, fmt.Errorf("plan stale-attempt retry for node_run=%q: %w", attempt.NodeRunID, err)
	}

	currentAttemptID, err := r.resolveAttemptID(ctx, attempt)
	if err != nil {
		return recoveryOutcome{}, err
	}
	if recovered, err := r.recoverProviderBackedAttemptFromSettlement(ctx, attempt, currentAttemptID, nowTime); err != nil {
		return recoveryOutcome{}, err
	} else if recovered {
		return recoveryOutcome{}, nil
	}
	if shouldSkip, err := r.shouldSkipStaleRecovery(ctx, attempt, currentAttemptID); err != nil {
		return recoveryOutcome{}, err
	} else if shouldSkip {
		return recoveryOutcome{}, nil
	}

	nextSeq, err := r.nextEventSeq(ctx, currentAttemptID)
	if err != nil {
		return recoveryOutcome{}, err
	}

	replacementAgentID, hasReplacement, err := r.findReplacementAgent(ctx, attempt.RequiredRole, attempt.AgentID)
	if err != nil {
		return recoveryOutcome{}, err
	}
	allowSelfRetry := plan.ShouldRetry && !hasReplacement && isPlanningNodeID(attempt.NodeID) && attempt.Attempt <= 1
	if allowSelfRetry {
		replacementAgentID = attempt.AgentID
	}

	if plan.ShouldRetry && (hasReplacement || allowSelfRetry) {
		if _, err := r.transition.ReassignStaleAttemptForRetry(ctx, ReassignStaleAttemptRequest{
			Attempt:            attempt,
			ReplacementAgentID: replacementAgentID,
			HasReplacement:     hasReplacement,
			OccurredAt:         nowTime.UTC().Format(time.RFC3339),
			CurrentAttemptID:   currentAttemptID,
			CurrentEventSeq:    nextSeq,
		}); err != nil {
			return recoveryOutcome{}, err
		}

		return recoveryOutcome{reassigned: true, agentBlocked: hasReplacement}, nil
	}

	if err := r.blockAgentIfNeeded(ctx, attempt.AgentID, nowTime); err != nil {
		return recoveryOutcome{}, err
	}

	staleSeconds := 0
	if startedAt, parseErr := time.Parse(time.RFC3339, strings.TrimSpace(attempt.StartedAt)); parseErr == nil {
		staleSeconds = int(nowTime.Sub(startedAt.UTC()).Seconds())
		if staleSeconds < 0 {
			staleSeconds = 0
		}
	}
	progressDiagnostics, err := r.latestAttemptProgressDiagnostics(ctx, currentAttemptID, attempt.NodeRunID, nowTime)
	if err != nil {
		return recoveryOutcome{}, err
	}
	payloadRaw, err := json.Marshal(map[string]any{
		"error":                    staleAttemptFailureReasonText,
		"node_run_id":              attempt.NodeRunID,
		"run_id":                   attempt.RunID,
		"node_id":                  attempt.NodeID,
		"candidate_started_at":     strings.TrimSpace(attempt.StartedAt),
		"evaluated_at":             nowTime.UTC().Format(time.RFC3339),
		"stale_threshold_seconds":  int(DefaultReassignCandidateThreshold.Seconds()),
		"planning_grace_seconds":   int(defaultPlanningStaleGraceWindow().Seconds()),
		"reason":                   "stale_attempt",
		"retry_allowed":            plan.ShouldRetry,
		"replacement_available":    hasReplacement,
		"retries_attempted":        maxInt(0, attempt.Attempt-1),
		"max_retries":              maxInt(0, plan.MaxRetries),
		"stale_duration_seconds":   staleSeconds,
		"reconciler_authority":     "system.reconciler",
		"planning_node_self_retry": allowSelfRetry,
		"latest_progress":          progressDiagnostics,
	})
	if err != nil {
		return recoveryOutcome{}, fmt.Errorf("marshal stale attempt failure payload: %w", err)
	}

	if err := r.transition.RecordDirectExecutionTerminal(ctx, DirectExecutionTerminalRequest{Transition: TransitionRequest{
		TransitionID:        fmt.Sprintf("tr_reconcile_%s_%s_%d_failed", attempt.RunID, attempt.NodeID, attempt.Attempt),
		EventID:             fmt.Sprintf("evt_reconcile_%s_%s_%d_failed", attempt.RunID, attempt.NodeID, attempt.Attempt),
		RunID:               attempt.RunID,
		NodeRunID:           sql.NullString{String: attempt.NodeRunID, Valid: true},
		Entity:              EntityTypeNode,
		FromState:           attempt.CurrentStatus,
		ToState:             "failed",
		ActorID:             "reconciler",
		RoleID:              attempt.RequiredRole,
		AuthoritySource:     "system.reconciler",
		ReasonCode:          string(errorcatalog.CodeAgentUnresponsive),
		ReasonText:          staleAttemptFailureReasonText,
		CorrelationID:       fmt.Sprintf("corr_reconcile_%s", attempt.RunID),
		CausationID:         fmt.Sprintf("caus_reconcile_%s_%s", attempt.RunID, attempt.NodeRunID),
		OccurredAt:          nowTime.UTC().Format(time.RFC3339),
		TransitionPrevHash:  "reconcile_transition_prev_hash",
		TransitionEntryHash: fmt.Sprintf("reconcile_transition_entry_hash_%s_failed", attempt.NodeRunID),
		EventPrevHash:       "reconcile_event_prev_hash",
		EventEntryHash:      fmt.Sprintf("reconcile_event_entry_hash_%s_failed", attempt.NodeRunID),
		AttemptID:           currentAttemptID,
		SchemaVersion:       "v1",
		EventType:           "attempt.failed",
		EventSeq:            nextSeq,
		PayloadJSON:         string(payloadRaw),
	}}); err != nil {
		return recoveryOutcome{}, err
	}

	if plan.DeadLetter {
		if err := r.insertEscalationEvent(ctx, attempt, plan, currentAttemptID, nowTime); err != nil {
			return recoveryOutcome{}, err
		}
	}

	return recoveryOutcome{failed: true, agentBlocked: true}, nil
}

func (r *Reconciler) shouldSkipStaleRecovery(ctx context.Context, attempt staleAttempt, attemptID string) (bool, error) {
	if r == nil || r.db == nil {
		return false, nil
	}
	trimmedAttemptID := strings.TrimSpace(attemptID)
	if trimmedAttemptID == "" {
		return false, nil
	}
	var state sql.NullString
	err := r.db.QueryRowContext(ctx,
		`SELECT state
		 FROM attempt_executions
		 WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?
		 LIMIT 1`,
		trimmedAttemptID,
		strings.TrimSpace(attempt.RunID),
		strings.TrimSpace(attempt.NodeRunID),
	).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load attempt execution state for stale recovery: %w", err)
	}
	switch strings.TrimSpace(state.String) {
	case "terminal_observed", "committed_success", "committed_failure", "committed_cancelled":
		return true, nil
	default:
		return false, nil
	}
}

func (r *Reconciler) convergeProviderSettledAttempts(ctx context.Context, nowTime time.Time) error {
	if r == nil || r.db == nil {
		return nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT ae.attempt_id,
	       nr.node_id
	FROM attempt_executions ae
	JOIN node_runs nr ON nr.node_run_id = ae.node_run_id AND nr.run_id = ae.run_id
	LEFT JOIN provider_invocations pi ON pi.provider_invocation_id = ae.active_provider_invocation_id
	LEFT JOIN provider_completion_settlements pcs ON pcs.provider_invocation_id = ae.active_provider_invocation_id
	WHERE ae.execution_mode = 'provider_backed'
	  AND ae.state NOT IN ('committed_success', 'committed_failure', 'committed_cancelled')
	  AND COALESCE(TRIM(ae.active_provider_invocation_id), '') <> ''
	  AND (
		pcs.settlement_id IS NOT NULL
		OR COALESCE(TRIM(pi.invocation_state), '') IN ('receipt_persisted', 'settled_success', 'settled_failed', 'settled_cancelled', 'invalid_handshake')
	  )
	ORDER BY COALESCE(ae.updated_at, ae.started_at, ae.created_at) ASC,
	         ae.attempt_id ASC`)
	if err != nil {
		return fmt.Errorf("query provider-settled attempts awaiting convergence: %w", err)
	}
	defer rows.Close()

	targets := make([]providerSettlementRecoveryCandidate, 0)
	for rows.Next() {
		var target providerSettlementRecoveryCandidate
		if err := rows.Scan(&target.AttemptID, &target.NodeID); err != nil {
			return fmt.Errorf("scan provider-settled convergence target: %w", err)
		}
		targets = append(targets, target)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate provider-settled convergence targets: %w", err)
	}

	for _, target := range targets {
		if _, err := r.convergeProviderBackedAttemptFromSettlement(ctx, target.AttemptID, target.NodeID, nowTime); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) recoverProviderBackedAttemptFromSettlement(ctx context.Context, attempt staleAttempt, attemptID string, nowTime time.Time) (bool, error) {
	return r.convergeProviderBackedAttemptFromSettlement(ctx, attemptID, attempt.NodeID, nowTime)
}

func (r *Reconciler) convergeProviderBackedAttemptFromSettlement(ctx context.Context, attemptID, nodeID string, nowTime time.Time) (bool, error) {
	if r == nil || r.db == nil {
		return false, nil
	}
	trimmedAttemptID := strings.TrimSpace(attemptID)
	if trimmedAttemptID == "" {
		return false, nil
	}
	store := executionstore.New(r.db)
	persisted, found, err := store.GetAttempt(ctx, trimmedAttemptID)
	if err != nil {
		return false, fmt.Errorf("load provider-backed attempt %q for stale recovery: %w", trimmedAttemptID, err)
	}
	if !found || persisted.ExecutionMode != executionstore.ExecutionModeProviderBacked || isAttemptExecutionTerminalState(strings.TrimSpace(persisted.State)) {
		return false, nil
	}
	providerInvocationID := strings.TrimSpace(persisted.ActiveProviderInvocationID)
	if providerInvocationID == "" {
		return false, nil
	}
	providerSessionID := strings.TrimSpace(persisted.ProviderSessionID)
	nodeClass, err := r.loadRunNodeClass(ctx, persisted.RunID, nodeID)
	if err != nil {
		return false, fmt.Errorf("load node class for provider settlement recovery attempt %q: %w", trimmedAttemptID, err)
	}

	settlement, foundSettlement, err := store.GetProviderCompletionSettlementByInvocation(ctx, providerInvocationID)
	if err != nil {
		return false, fmt.Errorf("load provider settlement for stale recovery attempt %q: %w", trimmedAttemptID, err)
	}
	if foundSettlement {
		settlement, err = reconcileRecoveredReceiptSettlement(ctx, store, persisted, nodeID, nodeClass, settlement, nowTime)
		if err != nil {
			return false, fmt.Errorf("reconcile recovered provider settlement for attempt %q: %w", trimmedAttemptID, err)
		}
	}
	if !foundSettlement {
		invocation, foundInvocation, err := store.GetProviderInvocation(ctx, providerInvocationID)
		if err != nil {
			return false, fmt.Errorf("load provider invocation %q for stale recovery attempt %q: %w", providerInvocationID, trimmedAttemptID, err)
		}
		if !foundInvocation {
			return false, nil
		}
		providerSessionID = firstNonEmptyTrimmed(providerSessionID, strings.TrimSpace(invocation.ProviderSessionID))
		switch invocation.InvocationState {
		case executionstore.ProviderInvocationStateReceiptPersisted, executionstore.ProviderInvocationStateSettledSuccess:
			receiptEvidence, foundReceipt, err := store.GetProviderCompletionReceiptEvidence(ctx, providerInvocationID)
			if err != nil {
				return false, fmt.Errorf("load provider completion receipt evidence for stale recovery attempt %q: %w", trimmedAttemptID, err)
			}
			if !foundReceipt {
				return false, fmt.Errorf("provider completion receipt evidence missing for stale recovery attempt %q invocation %q", trimmedAttemptID, providerInvocationID)
			}
			receiptPayload, err := executionstore.ParseProviderCompletionReceiptEvidencePayload(receiptEvidence.PayloadJSON)
			if err != nil {
				return false, fmt.Errorf("parse provider completion receipt evidence for stale recovery attempt %q: %w", trimmedAttemptID, err)
			}
			recoveredOutcome, failureReasonCode, failureReasonText, err := recoveredReceiptSettlementOutcome(nodeID, nodeClass, receiptPayload.FinalText, receiptPayload.StructuredPayloadJSON)
			if err != nil {
				return false, fmt.Errorf("adjudicate recovered provider receipt for stale recovery attempt %q: %w", trimmedAttemptID, err)
			}
			settledAt := firstNonEmptyTrimmed(strings.TrimSpace(receiptEvidence.CreatedAt), strings.TrimSpace(invocation.ReceiptPersistedAt), nowTime.UTC().Format(time.RFC3339))
			settlement, err = finalizeProviderCompletionSettlementWithRetry(ctx, store, executionstore.FinalizeProviderCompletionSettlementRequest{
				SettlementID:          executionstore.ProviderCompletionSettlementID(providerInvocationID),
				ProviderInvocationID:  providerInvocationID,
				AttemptID:             persisted.AttemptID,
				RunID:                 persisted.RunID,
				NodeRunID:             persisted.NodeRunID,
				Outcome:               recoveredOutcome,
				PrimaryEvidenceID:     receiptEvidence.EvidenceID,
				FinalText:             receiptPayload.FinalText,
				StructuredPayloadJSON: receiptPayload.StructuredPayloadJSON,
				FailureReasonCode:     failureReasonCode,
				FailureReasonText:     failureReasonText,
				SettledAt:             settledAt,
			})
			if err != nil {
				return false, fmt.Errorf("finalize provider settlement for stale recovery attempt %q: %w", trimmedAttemptID, err)
			}
		case executionstore.ProviderInvocationStateInvalidHandshake:
			settlement, err = reconcileInvalidHandshakeProviderSettlement(ctx, store, persisted, invocation, nowTime)
			if err != nil {
				return false, fmt.Errorf("finalize invalid-handshake provider settlement for stale recovery attempt %q: %w", trimmedAttemptID, err)
			}
		default:
			return false, nil
		}
	}
	terminalState, reasonCode := recoveredAttemptTerminalStateForSettlement(settlement)
	payload := map[string]any{
		"attempt_id":              strings.TrimSpace(persisted.AttemptID),
		"node_run_id":             strings.TrimSpace(persisted.NodeRunID),
		"provider_invocation_id":  strings.TrimSpace(providerInvocationID),
		"canonical_settlement_id": strings.TrimSpace(settlement.SettlementID),
		"provider_outcome":        strings.TrimSpace(string(settlement.Outcome)),
		"reason":                  "provider_settlement_recovered",
		"reconciler_authority":    "system.reconciler",
	}
	if trimmedNodeID := strings.TrimSpace(nodeID); trimmedNodeID != "" {
		payload["node_id"] = trimmedNodeID
	}
	payloadRaw, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("marshal stale recovery settlement payload for attempt %q: %w", trimmedAttemptID, err)
	}
	commitResult, err := store.CommitRecoveredTerminal(ctx, executionstore.CommitRecoveredTerminalRequest{
		AttemptID:          persisted.AttemptID,
		RunID:              persisted.RunID,
		NodeRunID:          persisted.NodeRunID,
		TerminalState:      terminalState,
		PayloadJSON:        string(payloadRaw),
		CommittedAt:        firstNonEmptyTrimmed(strings.TrimSpace(settlement.SettledAt), nowTime.UTC().Format(time.RFC3339)),
		TerminalReasonCode: reasonCode,
		FailureCode:        strings.TrimSpace(settlement.FailureReasonCode),
		FailureText:        strings.TrimSpace(settlement.FailureReasonText),
		ProviderSessionID:  providerSessionID,
	})
	if err != nil {
		return false, fmt.Errorf("commit recovered terminal state for attempt %q: %w", trimmedAttemptID, err)
	}
	if !isAttemptExecutionTerminalState(commitResult.AttemptState) {
		return false, nil
	}
	reconciledAttempt, found, err := store.GetAttempt(ctx, trimmedAttemptID)
	if err != nil {
		return false, fmt.Errorf("reload recovered attempt %q after terminal commit: %w", trimmedAttemptID, err)
	}
	if !found {
		return false, fmt.Errorf("recovered attempt %q not found after terminal commit", trimmedAttemptID)
	}
	if err := r.reconcileDurableAttemptProviderSettlement(ctx, store, reconciledAttempt, nowTime); err != nil {
		return false, fmt.Errorf("reconcile provider settlement after recovered terminal commit for attempt %q: %w", trimmedAttemptID, err)
	}
	return true, nil
}

func (r *Reconciler) loadRunNodeClass(ctx context.Context, runID, nodeID string) (string, error) {
	if r == nil || r.db == nil {
		return "", nil
	}
	trimmedRunID := strings.TrimSpace(runID)
	trimmedNodeID := strings.TrimSpace(nodeID)
	if trimmedRunID == "" || trimmedNodeID == "" {
		return "", nil
	}
	var nodeClass sql.NullString
	err := r.db.QueryRowContext(ctx, `SELECT NULLIF(TRIM(wn.node_class), '')
FROM workflow_runs wr
JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id
WHERE wr.run_id = ? AND wn.node_id = ?
LIMIT 1`, trimmedRunID, trimmedNodeID).Scan(&nodeClass)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(nodeClass.String), nil
}

func reconcileRecoveredReceiptSettlement(ctx context.Context, store *executionstore.Store, attempt executionstore.AttemptExecution, nodeID, nodeClass string, settlement executionstore.ProviderCompletionSettlement, nowTime time.Time) (executionstore.ProviderCompletionSettlement, error) {
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeSuccess {
		return settlement, nil
	}
	recoveredOutcome, failureReasonCode, failureReasonText, err := recoveredReceiptSettlementOutcome(nodeID, nodeClass, settlement.FinalText, settlement.StructuredPayloadJSON)
	if err != nil {
		return executionstore.ProviderCompletionSettlement{}, err
	}
	if recoveredOutcome == executionstore.ProviderCompletionOutcomeSuccess {
		return settlement, nil
	}
	return reconcileProviderCompletionSettlementWithRetry(ctx, store, executionstore.ReconcileProviderCompletionSettlementRequest{
		ProviderInvocationID: settlement.ProviderInvocationID,
		AttemptID:            attempt.AttemptID,
		RunID:                attempt.RunID,
		NodeRunID:            attempt.NodeRunID,
		Outcome:              recoveredOutcome,
		FailureReasonCode:    failureReasonCode,
		FailureReasonText:    failureReasonText,
		SettledAt:            firstNonEmptyTrimmed(strings.TrimSpace(settlement.SettledAt), nowTime.UTC().Format(time.RFC3339)),
	})
}

func reconcileInvalidHandshakeProviderSettlement(ctx context.Context, store *executionstore.Store, attempt executionstore.AttemptExecution, invocation executionstore.ProviderInvocation, nowTime time.Time) (executionstore.ProviderCompletionSettlement, error) {
	failureReasonCode, failureReasonText := invalidHandshakeFailure(invocation.InvalidReason)
	outputText := ""
	if receiptEvidence, foundReceipt, err := store.GetProviderCompletionReceiptEvidence(ctx, invocation.ProviderInvocationID); err != nil {
		return executionstore.ProviderCompletionSettlement{}, fmt.Errorf("load provider completion receipt evidence for invalid handshake %q: %w", invocation.ProviderInvocationID, err)
	} else if foundReceipt {
		receiptPayload, parseErr := executionstore.ParseProviderCompletionReceiptEvidencePayload(receiptEvidence.PayloadJSON)
		if parseErr != nil {
			return executionstore.ProviderCompletionSettlement{}, fmt.Errorf("parse provider completion receipt evidence for invalid handshake %q: %w", invocation.ProviderInvocationID, parseErr)
		}
		outputText = receiptPayload.FinalText
	}
	settledAt := firstNonEmptyTrimmed(strings.TrimSpace(invocation.UpdatedAt), strings.TrimSpace(invocation.ReceiptPersistedAt), nowTime.UTC().Format(time.RFC3339))
	terminalEvidence, err := ensureTerminalProviderCompletionEvidenceWithRetry(ctx, store, executionstore.EnsureTerminalProviderCompletionEvidenceRequest{
		ProviderInvocationID: invocation.ProviderInvocationID,
		AttemptID:            attempt.AttemptID,
		RunID:                attempt.RunID,
		NodeRunID:            attempt.NodeRunID,
		State:                "failed",
		ReasonCode:           failureReasonCode,
		FailureText:          failureReasonText,
		OutputText:           outputText,
		CreatedAt:            settledAt,
	})
	if err != nil {
		return executionstore.ProviderCompletionSettlement{}, fmt.Errorf("ensure terminal provider evidence for invalid handshake %q: %w", invocation.ProviderInvocationID, err)
	}
	return finalizeProviderCompletionSettlementWithRetry(ctx, store, executionstore.FinalizeProviderCompletionSettlementRequest{
		SettlementID:          executionstore.ProviderCompletionSettlementID(invocation.ProviderInvocationID),
		ProviderInvocationID:  invocation.ProviderInvocationID,
		AttemptID:             attempt.AttemptID,
		RunID:                 attempt.RunID,
		NodeRunID:             attempt.NodeRunID,
		Outcome:               executionstore.ProviderCompletionOutcomeFailed,
		PrimaryEvidenceID:     terminalEvidence.EvidenceID,
		FailureReasonCode:     failureReasonCode,
		FailureReasonText:     failureReasonText,
		StructuredPayloadJSON: `{}`,
		SettledAt:             settledAt,
	})
}

func invalidHandshakeFailure(invalidReason string) (string, string) {
	switch strings.TrimSpace(invalidReason) {
	case "completion_receipt_conflict":
		return "provider_incomplete_turn", "conflicting provider completion receipts invalidated the invocation"
	case "completion_capability_mismatch":
		return "provider_incomplete_turn", "provider completion capability mismatch invalidated the invocation"
	case "attempt_lineage_mismatch":
		return "provider_incomplete_turn", "provider completion attempt identity did not match the active invocation"
	default:
		return "provider_incomplete_turn", "provider completion handshake invalidated the invocation"
	}
}

func recoveredReceiptSettlementOutcome(nodeID, nodeClass, finalText, structuredPayloadJSON string) (executionstore.ProviderCompletionOutcome, string, string, error) {
	decision, handled, err := coreverdict.ReceiptDecisionForNode(nodeID, nodeClass, structuredPayloadJSON, finalText)
	if err != nil {
		var contractErr *coreverdict.ReceiptContractError
		if errors.As(err, &contractErr) {
			return executionstore.ProviderCompletionOutcomeFailed, strings.TrimSpace(contractErr.ReasonCode), strings.TrimSpace(contractErr.Error()), nil
		}
		return "", "", "", err
	}
	if handled && strings.TrimSpace(decision.FailureReasonCode) != "" {
		return executionstore.ProviderCompletionOutcomeFailed, strings.TrimSpace(decision.FailureReasonCode), strings.TrimSpace(decision.FailureReasonText), nil
	}
	return executionstore.ProviderCompletionOutcomeSuccess, "", "", nil
}

func recoveredAttemptTerminalStateForSettlement(settlement executionstore.ProviderCompletionSettlement) (string, string) {
	switch settlement.Outcome {
	case executionstore.ProviderCompletionOutcomeCancelled:
		return "committed_cancelled", firstNonEmptyTrimmed(strings.TrimSpace(settlement.FailureReasonCode), "run_terminalized")
	case executionstore.ProviderCompletionOutcomeFailed:
		return "committed_failure", firstNonEmptyTrimmed(strings.TrimSpace(settlement.FailureReasonCode), "internal_error")
	default:
		return "committed_success", "execution_completed"
	}
}

func isAttemptExecutionTerminalState(state string) bool {
	switch strings.TrimSpace(state) {
	case "committed_success", "committed_failure", "committed_cancelled":
		return true
	default:
		return false
	}
}

func (r *Reconciler) latestAttemptProgressDiagnostics(ctx context.Context, attemptID, nodeRunID string, now time.Time) (map[string]any, error) {
	attemptID = strings.TrimSpace(attemptID)
	nodeRunID = strings.TrimSpace(nodeRunID)
	if attemptID == "" {
		return map[string]any{}, nil
	}

	var createdAt sql.NullString
	var payloadRaw sql.NullString
	var progressCount int
	if err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(1)
		 FROM events
		 WHERE attempt_id = ? AND event_type = 'attempt.progress'`,
		attemptID,
	).Scan(&progressCount); err != nil {
		return nil, fmt.Errorf("count latest attempt progress diagnostics %q: %w", attemptID, err)
	}
	if progressCount == 0 && nodeRunID != "" {
		if err := r.db.QueryRowContext(ctx,
			`SELECT COUNT(1)
			 FROM events
			 WHERE node_run_id = ? AND event_type = 'attempt.progress'`,
			nodeRunID,
		).Scan(&progressCount); err != nil {
			return nil, fmt.Errorf("count latest attempt progress diagnostics node_run=%q: %w", nodeRunID, err)
		}
	}
	if progressCount == 0 {
		return map[string]any{"attempt_id": attemptID, "node_run_id": nodeRunID, "progress_event_seen": false}, nil
	}

	err := r.db.QueryRowContext(ctx,
		`SELECT created_at, payload_json
		 FROM events
		 WHERE attempt_id = ? AND event_type = 'attempt.progress'
		 ORDER BY seq DESC
		 LIMIT 1`,
		attemptID,
	).Scan(&createdAt, &payloadRaw)
	if errors.Is(err, sql.ErrNoRows) && nodeRunID != "" {
		err = r.db.QueryRowContext(ctx,
			`SELECT created_at, payload_json
			 FROM events
			 WHERE node_run_id = ? AND event_type = 'attempt.progress'
			 ORDER BY seq DESC
			 LIMIT 1`,
			nodeRunID,
		).Scan(&createdAt, &payloadRaw)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return map[string]any{"attempt_id": attemptID, "node_run_id": nodeRunID, "progress_event_seen": false}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query latest attempt progress diagnostics %q: %w", attemptID, err)
	}

	result := map[string]any{
		"attempt_id":          attemptID,
		"progress_event_seen": true,
	}
	if createdAt.Valid {
		result["last_progress_at"] = strings.TrimSpace(createdAt.String)
		if parsed, parseErr := time.Parse(time.RFC3339, strings.TrimSpace(createdAt.String)); parseErr == nil {
			delta := now.Sub(parsed.UTC()).Milliseconds()
			if delta < 0 {
				delta = 0
			}
			result["elapsed_since_last_progress_ms"] = delta
		}
	}

	if payloadRaw.Valid && strings.TrimSpace(payloadRaw.String) != "" {
		var payload map[string]any
		if err := json.Unmarshal([]byte(payloadRaw.String), &payload); err == nil {
			if executionDiagnostics, ok := payload["execution_diagnostics"]; ok {
				result["execution_diagnostics"] = executionDiagnostics
				if diagMap, ok := executionDiagnostics.(map[string]any); ok {
					if observedAt, ok := diagMap["observed_at"]; ok {
						result["last_observation_at"] = observedAt
					}
					if terminalSeen, ok := diagMap["terminal_event_seen"]; ok {
						result["terminal_event_seen_in_stream"] = terminalSeen
					}
					if stepFinishSeen, ok := diagMap["step_finish_seen"]; ok {
						result["step_finish_seen"] = stepFinishSeen
					}
				}
			}
			if text, ok := payload["text"].(string); ok {
				trimmed := strings.TrimSpace(text)
				result["last_progress_text_len"] = len(trimmed)
				if len(trimmed) > 0 {
					if len(trimmed) > 600 {
						trimmed = trimmed[:600]
					}
					result["last_progress_text_excerpt"] = trimmed
				}
			}
			if snapshot, ok := payload["execution_path_diagnostics_snapshot"]; ok {
				result["execution_path_diagnostics_snapshot"] = snapshot
			}
		}
	}

	return result, nil
}

func CountStaleAttemptsForMetrics(ctx context.Context, q staleAttemptQuerier, nowTime time.Time, threshold time.Duration) (int, error) {
	if q == nil {
		return 0, fmt.Errorf("stale attempt querier is required")
	}
	stale, err := findStaleAttemptsWithQuerier(ctx, q, nowTime, threshold)
	if err != nil {
		return 0, err
	}
	return len(stale), nil
}

func (r *Reconciler) findStaleAttempts(ctx context.Context, nowTime time.Time, threshold time.Duration) ([]staleAttempt, error) {
	return findStaleAttemptsWithQuerier(ctx, r.db, nowTime, threshold)
}

func findStaleAttemptsWithQuerier(ctx context.Context, q staleAttemptQuerier, nowTime time.Time, threshold time.Duration) ([]staleAttempt, error) {
	if q == nil {
		return nil, fmt.Errorf("stale attempt querier is required")
	}
	if threshold <= 0 {
		threshold = DefaultReassignCandidateThreshold
	}
	cutoff := nowTime.Add(-threshold).UTC().Format(time.RFC3339)
	query := fmt.Sprintf(`SELECT n.node_run_id,
		        n.run_id,
		        n.node_id,
		        n.attempt,
		        n.wave,
		        n.required_role,
		        n.agent_id,
		        COALESCE(ae.session_ref, ''),
		        COALESCE(ae.state, ''),
		        n.node_status,
		        CASE
		          WHEN n.node_status = 'starting' THEN COALESCE(ae.launch_started_at, '')
		          ELSE COALESCE(n.started_at, '')
		        END AS candidate_started_at
		 FROM node_runs n
		 JOIN agents a ON a.agent_id = n.agent_id
		 LEFT JOIN attempt_executions ae ON ae.attempt_id = n.run_id || ':' || n.node_id || ':' || n.attempt
		 LEFT JOIN session_leases sl ON sl.session_id = ae.session_ref AND sl.status = 'active'
		 WHERE n.node_status IN ('starting', 'running')
		   AND (
		     (
		       n.node_status = 'starting'
		       AND COALESCE(ae.state, '') = 'starting'
		       AND COALESCE(ae.launch_started_at, '') != ''
		       AND ae.launch_started_at <= ?
		       AND (
		         sl.session_id IS NULL OR
		         %s
		       )
		     ) OR (
		       n.node_status = 'running'
		       AND n.started_at IS NOT NULL
		       AND COALESCE(ae.state, '') NOT IN ('starting', 'terminal_observed', 'committed_success', 'committed_failure', 'committed_cancelled')
		       AND n.started_at <= ?
		     )
		   )
		 ORDER BY n.run_id ASC, n.node_id ASC, n.attempt ASC`, staleActiveSessionLeaseConditionSQL("sl"))

	rows, err := q.QueryContext(ctx,
		query,
		cutoff,
		cutoff,
		nowTime.UTC().Format(time.RFC3339),
		cutoff,
	)
	if err != nil {
		return nil, fmt.Errorf("query stale attempts: %w", err)
	}
	defer rows.Close()

	candidates := make([]staleAttempt, 0)
	for rows.Next() {
		var candidate staleAttempt
		if err := rows.Scan(
			&candidate.NodeRunID,
			&candidate.RunID,
			&candidate.NodeID,
			&candidate.Attempt,
			&candidate.Wave,
			&candidate.RequiredRole,
			&candidate.AgentID,
			&candidate.SessionRef,
			&candidate.AttemptState,
			&candidate.CurrentStatus,
			&candidate.StartedAt,
		); err != nil {
			return nil, fmt.Errorf("scan stale attempt: %w", err)
		}
		candidates = append(candidates, candidate)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stale attempts: %w", err)
	}

	stale := make([]staleAttempt, 0, len(candidates))
	for _, candidate := range candidates {
		if isPlanningNodeID(candidate.NodeID) {
			planningCutoff := nowTime.Add(-(threshold + defaultPlanningStaleGraceWindow())).UTC()
			if startedAt, parseErr := time.Parse(time.RFC3339, strings.TrimSpace(candidate.StartedAt)); parseErr == nil {
				if startedAt.UTC().After(planningCutoff) {
					continue
				}
			}
		}
		grace := threshold
		if grace < defaultProgressStaleGrace {
			grace = defaultProgressStaleGrace
		}
		skip, err := HasRecentExecutionActivity(ctx, q, candidate.NodeRunID, nowTime, grace)
		if err != nil {
			return nil, err
		}
		if skip {
			continue
		}
		stale = append(stale, candidate)
	}

	return stale, nil
}

func defaultPlanningStaleGraceWindow() time.Duration {
	return 4 * time.Minute
}

func isPlanningNodeID(nodeID string) bool {
	trimmed := strings.TrimSpace(nodeID)
	if trimmed == "" {
		return false
	}
	if strings.HasPrefix(trimmed, "advisor_consultation_") {
		return true
	}
	switch trimmed {
	case "decree_intake", "advisory_synthesis", "plan_drafting", "king_plan_decision":
		return true
	default:
		return false
	}
}

func retryStrategyLabel(hasReplacement bool, allowSelfRetry bool) string {
	if hasReplacement {
		return "replacement_agent"
	}
	if allowSelfRetry {
		return "same_agent"
	}
	return "none"
}

func (r *Reconciler) findReplacementAgent(ctx context.Context, roleID string, excludedAgentID string) (string, bool, error) {
	var agentID string
	err := r.db.QueryRowContext(ctx,
		`SELECT agent_id
		 FROM agents a
		 WHERE a.role_id = ?
		   AND a.agent_id <> ?
		   AND a.status NOT IN ('offline', 'failed')
		   AND NOT EXISTS (
		     SELECT 1
		     FROM node_runs nr
		     WHERE nr.agent_id = a.agent_id
		       AND COALESCE(TRIM(nr.superseded_at), '') = ''
		       AND nr.node_status IN ('pending', 'ready', 'starting', 'running', 'blocked')
		   )
		 ORDER BY CASE a.status
		   WHEN 'idle' THEN 0
		   WHEN 'busy' THEN 1
		   WHEN 'blocked' THEN 2
		   ELSE 3
		 END,
		 a.agent_id ASC
		 LIMIT 1`,
		roleID,
		excludedAgentID,
	).Scan(&agentID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("query replacement agent for role %q: %w", roleID, err)
	}

	return agentID, true, nil
}

func (r *Reconciler) blockAgentIfNeeded(ctx context.Context, agentID string, nowTime time.Time) error {
	status, found, err := r.registry.agentStatus(ctx, agentID)
	if err != nil {
		return err
	}
	if !found {
		return &AgentNotFoundError{AgentID: agentID}
	}
	if status == AgentStatusBlocked || status == AgentStatusOffline || status == AgentStatusFailed {
		return nil
	}

	if err := r.registry.UpdateAgentStatus(ctx, UpdateAgentStatusRequest{
		AgentID:    agentID,
		FromStatus: status,
		ToStatus:   AgentStatusBlocked,
		UpdatedAt:  nowTime.UTC().Format(time.RFC3339),
	}); err != nil {
		var conflictErr *AgentStatusConflictError
		if errors.As(err, &conflictErr) {
			return nil
		}
		return err
	}

	return nil
}

func (r *Reconciler) resolveAttemptID(ctx context.Context, attempt staleAttempt) (string, error) {
	var attemptID string
	err := r.db.QueryRowContext(ctx,
		`SELECT attempt_id
		 FROM events
		 WHERE node_run_id = ?
		 ORDER BY seq DESC
		 LIMIT 1`,
		attempt.NodeRunID,
	).Scan(&attemptID)
	if errors.Is(err, sql.ErrNoRows) {
		return composeAttemptID(attempt.RunID, attempt.NodeID, attempt.Attempt), nil
	}
	if err != nil {
		return "", fmt.Errorf("resolve attempt id for node_run=%q: %w", attempt.NodeRunID, err)
	}
	return attemptID, nil
}

func (r *Reconciler) nextEventSeq(ctx context.Context, attemptID string) (int, error) {
	var seq int
	err := r.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0)
		 FROM events
		 WHERE attempt_id = ?`,
		attemptID,
	).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("query next event sequence for attempt=%q: %w", attemptID, err)
	}
	return seq + 1, nil
}

func (r *Reconciler) insertEscalationEvent(ctx context.Context, attempt staleAttempt, plan retryengine.PlanResult, attemptID string, nowTime time.Time) error {
	targetRole := strings.TrimSpace(plan.EscalationTarget)
	if targetRole == "" {
		targetRole = attempt.RequiredRole
	}

	exists, err := roleExists(ctx, r.db, targetRole)
	if err != nil {
		return err
	}
	if !exists {
		targetRole = attempt.RequiredRole
	}

	nextSeq, err := r.nextEventSeq(ctx, attemptID)
	if err != nil {
		return err
	}

	_, err = r.eventIngestor.Ingest(ctx, AttemptEventIngestionRequest{
		EventID:         fmt.Sprintf("evt_reconcile_%s_%s_%d_dead_letter", attempt.RunID, attempt.NodeID, attempt.Attempt),
		RunID:           attempt.RunID,
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: true},
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "escalation.raised",
		Seq:             nextSeq,
		PayloadJSON:     fmt.Sprintf(`{"run_id":%q,"node_id":%q,"attempt":%d,"reason":"%s","target_role":%q}`, attempt.RunID, attempt.NodeID, attempt.Attempt, plan.EscalationReason, targetRole),
		ActorID:         "reconciler",
		RoleID:          targetRole,
		AuthoritySource: "system.reconciler",
		ReasonCode:      string(errorcatalog.CodeAgentUnresponsive),
		ReasonText:      "stale running attempt exceeded retry policy; escalation raised",
		CorrelationID:   fmt.Sprintf("corr_reconcile_%s", attempt.RunID),
		CausationID:     fmt.Sprintf("caus_reconcile_%s_%s_dead_letter", attempt.RunID, attempt.NodeRunID),
		CreatedAt:       nowTime.UTC().Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("insert dead-letter escalation event for node_run=%q: %w", attempt.NodeRunID, err)
	}

	return nil
}

func normalizeReconcileInputs(req ReconcileRequest, nowFn func() time.Time) (time.Time, time.Duration, time.Duration, error) {
	var nowTime time.Time
	if strings.TrimSpace(req.Now) == "" {
		nowTime = nowFn().UTC()
	} else {
		parsed, err := time.Parse(time.RFC3339, req.Now)
		if err != nil {
			return time.Time{}, 0, 0, fmt.Errorf("parse reconcile now timestamp: %w", err)
		}
		nowTime = parsed.UTC()
	}

	reassignThreshold := req.ReassignCandidateThreshold
	if reassignThreshold <= 0 {
		reassignThreshold = DefaultReassignCandidateThreshold
	}

	staleHeartbeatThreshold := req.StaleHeartbeatThreshold
	if staleHeartbeatThreshold <= 0 {
		staleHeartbeatThreshold = DefaultStaleHeartbeatThreshold
	}

	return nowTime, reassignThreshold, staleHeartbeatThreshold, nil
}

func roleExists(ctx context.Context, db *sql.DB, roleID string) (bool, error) {
	var found string
	err := db.QueryRowContext(ctx,
		`SELECT role_id FROM roles WHERE role_id = ?`,
		roleID,
	).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("query role existence for %q: %w", roleID, err)
	}
	return true, nil
}

func composeAttemptID(runID, nodeID string, attempt int) string {
	return fmt.Sprintf("%s:%s:%d", runID, nodeID, attempt)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
