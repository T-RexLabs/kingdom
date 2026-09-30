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
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/core/retryengine"
	"kingdom/internal/core/rolecomm"
	"kingdom/internal/core/schemas"
	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/timestamporder"
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
		registry:       NewAgentRegistry(db),
		sessionManager: sessionManager,
		retryPlanner:   planner,
		now:            time.Now,
	}
}

func (r *Reconciler) RunStartup(ctx context.Context) (ReconcileResult, error) {
	return r.Reconcile(ctx, ReconcileRequest{Now: r.now().UTC().Format(time.RFC3339Nano)})
}

func (r *Reconciler) Reconcile(ctx context.Context, req ReconcileRequest) (ReconcileResult, error) {
	nowTime, reassignThreshold, staleHeartbeatThreshold, err := normalizeReconcileInputs(req, r.now)
	if err != nil {
		return ReconcileResult{}, err
	}

	if err := r.convergeProviderSettledAttempts(ctx, nowTime); err != nil {
		return ReconcileResult{}, err
	}
	if err := r.recoverUnprojectedAutomaticWorkOrderRetries(ctx, nowTime); err != nil {
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

	if err := r.projectDurableTerminalNodes(ctx, nowTime); err != nil {
		return ReconcileResult{}, err
	}

	staleLeases, err := r.sessionManager.FindStaleLeases(ctx, FindStaleSessionLeasesRequest{
		Now:                     nowTime.UTC().Format(time.RFC3339Nano),
		StaleHeartbeatThreshold: staleHeartbeatThreshold,
	})
	if err != nil {
		return ReconcileResult{}, err
	}

	for _, lease := range staleLeases {
		if _, blocked := blockedAgents[lease.AgentID]; blocked {
			_, closeErr := r.sessionManager.CloseLease(ctx, CloseSessionLeaseRequest{
				SessionID: lease.SessionID,
				UpdatedAt: nowTime.UTC().Format(time.RFC3339Nano),
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
				UpdatedAt: nowTime.UTC().Format(time.RFC3339Nano),
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
			LeaseExpiresAt:  nowTime.Add(staleHeartbeatThreshold).UTC().Format(time.RFC3339Nano),
			LastHeartbeatAt: nowTime.UTC().Format(time.RFC3339Nano),
			UpdatedAt:       nowTime.UTC().Format(time.RFC3339Nano),
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

func (r *Reconciler) recoverUnprojectedAutomaticWorkOrderRetries(ctx context.Context, now time.Time) error {
	rows, err := r.db.QueryContext(ctx, `SELECT ae.attempt_id, ae.run_id, ae.node_run_id, nr.node_id, nr.required_role, nr.node_status, COALESCE(ae.failure_code, ''), COALESCE(pcs.provider_diagnostics_json, '{}'), COALESCE(pce.payload_json, '{}'), COALESCE(ae.committed_at, ae.updated_at)
		FROM attempt_executions ae
		JOIN node_runs nr ON nr.node_run_id = ae.node_run_id AND nr.run_id = ae.run_id
		JOIN workflow_runs wr ON wr.run_id = ae.run_id
		LEFT JOIN provider_completion_settlements pcs ON pcs.settlement_id = ae.canonical_settlement_id
		LEFT JOIN provider_completion_evidence pce ON pce.provider_invocation_id = ae.active_provider_invocation_id AND pce.evidence_kind = 'stream_observation'
		LEFT JOIN retry_decisions rd ON rd.source_attempt_id = ae.attempt_id AND rd.subject_kind = 'work_order_attempt'
		WHERE ae.execution_mode = 'provider_backed' AND ae.state = 'committed_failure' AND nr.node_status IN ('starting', 'running') AND wr.run_status = 'running' AND rd.retry_decision_id IS NULL
		  AND COALESCE(ae.failure_code, '') NOT IN ('execution_review_rework', 'validation_gate_rework')
		ORDER BY ae.committed_at, ae.attempt_id`)
	if err != nil {
		return fmt.Errorf("list unprojected automatic work order retries: %w", err)
	}
	defer rows.Close()
	type candidate struct {
		attemptID, runID, nodeRunID, nodeID, roleID, nodeStatus, code, diagnostics, evidence, settledAt string
	}
	var candidates []candidate
	for rows.Next() {
		var candidate candidate
		if err := rows.Scan(&candidate.attemptID, &candidate.runID, &candidate.nodeRunID, &candidate.nodeID, &candidate.roleID, &candidate.nodeStatus, &candidate.code, &candidate.diagnostics, &candidate.evidence, &candidate.settledAt); err != nil {
			return fmt.Errorf("scan unprojected automatic work order retry: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, candidate := range candidates {
		settledAt, err := time.Parse(time.RFC3339Nano, candidate.settledAt)
		if err != nil {
			return fmt.Errorf("parse automatic retry settlement timestamp: %w", err)
		}
		var evidence struct {
			Observation providerexecution.InvocationStreamObservation `json:"observation"`
		}
		_ = json.Unmarshal([]byte(candidate.evidence), &evidence)
		count, err := r.transition.CountWorkOrderRetryDecisions(ctx, candidate.runID, candidate.nodeID)
		if err != nil {
			return err
		}
		code := canonicalRetryDecisionCode(candidate.code)
		plan, err := r.retryPlanner.Plan(code, count)
		if err != nil {
			return err
		}
		kind := RetryDecisionFailFast
		if plan.ShouldRetry && !evidence.Observation.MeaningfulProgress {
			kind = RetryDecisionRetry
		} else if plan.DeadLetter && !evidence.Observation.MeaningfulProgress {
			kind = RetryDecisionExhausted
		}
		retryIndex := count
		if kind == RetryDecisionRetry {
			retryIndex++
		}
		if retryIndex > plan.MaxRetries {
			retryIndex = plan.MaxRetries
		}
		providerRetryAt := providerexecution.RetryAtFromDiagnostics(candidate.diagnostics, settledAt)
		dueAt := settledAt.UTC().Add(plan.NextDelay)
		if providerRetryAt != nil && providerRetryAt.After(dueAt) {
			dueAt = *providerRetryAt
		}
		eventSeq, err := r.nextEventSeq(ctx, candidate.attemptID)
		if err != nil {
			return err
		}
		_, _, err = r.transition.RecordAutomaticWorkOrderRetry(ctx, AutomaticWorkOrderRetryRecordRequest{Transition: TransitionRequest{TransitionID: fmt.Sprintf("tr_reconcile_%s_retry_projection", candidate.attemptID), EventID: fmt.Sprintf("evt_reconcile_%s_retry_projection", candidate.attemptID), RunID: candidate.runID, NodeRunID: sql.NullString{String: candidate.nodeRunID, Valid: true}, Entity: EntityTypeNode, FromState: candidate.nodeStatus, ToState: "failed", ActorID: "reconciler", RoleID: candidate.roleID, AuthoritySource: "system.reconciler", ReasonCode: string(code), ReasonText: "recovered settled provider failure", CorrelationID: "corr_reconcile_" + candidate.runID, CausationID: "caus_reconcile_" + candidate.attemptID, OccurredAt: now.UTC().Format(time.RFC3339Nano), AttemptID: candidate.attemptID, SchemaVersion: "v1", EventType: "attempt.failed", EventSeq: eventSeq, PayloadJSON: `{}`}, Decision: RetryDecisionCreateRequest{DecisionID: fmt.Sprintf("retry_decision_%s", strings.ReplaceAll(candidate.attemptID, ":", "_")), SubjectKind: RetrySubjectKindWorkOrderAttempt, RunID: candidate.runID, NodeID: candidate.nodeID, NodeRunID: candidate.nodeRunID, SourceAttemptID: candidate.attemptID, PolicyID: retryPlannerPolicyID(r.retryPlanner), RetryClass: plan.RetryClass, ErrorCode: string(code), OperatorText: automaticRetryDecisionText(kind, evidence.Observation.MeaningfulProgress), MeaningfulProgress: evidence.Observation.MeaningfulProgress, Decision: kind, RetryIndex: retryIndex, MaxRetries: plan.MaxRetries, PolicyDelay: plan.NextDelay, ProviderRetryAt: providerRetryAt, DueAt: dueAt, CreatedAt: settledAt.UTC(), EscalationTarget: plan.EscalationTarget}})
		if err != nil {
			return err
		}
	}
	return nil
}

func canonicalRetryDecisionCode(raw string) errorcatalog.Code {
	code := errorcatalog.Code(strings.TrimSpace(raw))
	if _, ok := errorcatalog.Lookup(code); ok {
		return code
	}
	return errorcatalog.CodeProviderUnknownFailure
}

func automaticRetryDecisionText(kind RetryDecisionKind, meaningfulProgress bool) string {
	if meaningfulProgress {
		return "The provider made meaningful progress, so automatic retry is disabled."
	}
	if kind == RetryDecisionRetry {
		return "The provider failure is eligible for one delayed automatic retry."
	}
	if kind == RetryDecisionExhausted {
		return "The provider retry limit is exhausted and has been escalated."
	}
	return "The provider failure is classified fail-fast and will not retry automatically."
}

func retryPlannerPolicyID(planner retryPlanner) string {
	if withPolicyID, ok := planner.(interface{ PolicyID() string }); ok && strings.TrimSpace(withPolicyID.PolicyID()) != "" {
		return strings.TrimSpace(withPolicyID.PolicyID())
	}
	return "retry-policy"
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
	  -- A fenced stale attempt is durably cancelled but remains the visible
	  -- authority until its cancellation handoff releases the replacement.
	  AND NOT (state = 'committed_cancelled' AND node_status = 'blocked' AND EXISTS (
	    SELECT 1
	    FROM node_runs replacement
	    WHERE replacement.handoff_source_attempt_id = latest_attempts.attempt_id
	      AND replacement.node_status IN ('pending', 'ready')
	  ))
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
	NodeRunID      string
	RunID          string
	NodeID         string
	NodeClass      string
	Attempt        int
	Wave           int
	RequiredRole   string
	AgentID        string
	SessionRef     string
	AttemptState   string
	ExecutionMode  executionstore.ExecutionMode
	LeaseOwner     string
	LeaseVersion   int64
	LeaseExpiresAt string
	CurrentStatus  string
	StartedAt      string
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
	meaningfulProgress, err := r.hasMeaningfulStaleAttemptProgress(ctx, currentAttemptID)
	if err != nil {
		return recoveryOutcome{}, err
	}
	retriesAttempted, err := r.transition.CountWorkOrderRetryDecisions(ctx, attempt.RunID, attempt.NodeID)
	if err != nil {
		return recoveryOutcome{}, err
	}
	plan, err := r.retryPlanner.Plan(errorcatalog.CodeAgentUnresponsive, retriesAttempted)
	if err != nil {
		return recoveryOutcome{}, fmt.Errorf("plan stale-attempt retry for node_run=%q: %w", attempt.NodeRunID, err)
	}

	nextSeq, err := r.nextEventSeq(ctx, currentAttemptID)
	if err != nil {
		return recoveryOutcome{}, err
	}

	replacementAgentID, hasReplacement, err := r.findReplacementAgent(ctx, attempt.RequiredRole, attempt.AgentID)
	if err != nil {
		return recoveryOutcome{}, err
	}
	// A same-agent execution retry remains fenced behind the cancellation and
	// workspace handoff. Planning retains its narrower first-attempt policy.
	allowSelfRetry := plan.ShouldRetry && !meaningfulProgress && !hasReplacement && (!isPlanningNodeClass(workflow.ResolveNodeClass(attempt.NodeID, attempt.NodeClass)) || attempt.Attempt <= 1)
	if allowSelfRetry {
		replacementAgentID = attempt.AgentID
	}

	if plan.ShouldRetry && !meaningfulProgress && (hasReplacement || allowSelfRetry) {
		dueAt := nowTime.UTC().Add(plan.NextDelay)
		var retryDecision *RetryDecisionCreateRequest
		if plan.RetryClass != "" && plan.EscalationTarget != "" {
			retryDecision = &RetryDecisionCreateRequest{
				DecisionID:       fmt.Sprintf("retry_decision_%s", strings.ReplaceAll(currentAttemptID, ":", "_")),
				SubjectKind:      RetrySubjectKindWorkOrderAttempt,
				RunID:            attempt.RunID,
				NodeID:           attempt.NodeID,
				NodeRunID:        attempt.NodeRunID,
				SourceAttemptID:  currentAttemptID,
				PolicyID:         retryPlannerPolicyID(r.retryPlanner),
				RetryClass:       plan.RetryClass,
				ErrorCode:        string(errorcatalog.CodeAgentUnresponsive),
				OperatorText:     "The stale provider attempt is eligible for one delayed automatic retry.",
				Decision:         RetryDecisionRetry,
				RetryIndex:       retriesAttempted + 1,
				MaxRetries:       plan.MaxRetries,
				PolicyDelay:      plan.NextDelay,
				DueAt:            dueAt,
				CreatedAt:        nowTime.UTC(),
				EscalationTarget: plan.EscalationTarget,
			}
		}
		queued, err := r.transition.ScheduleFencedStaleRetry(ctx, ScheduleFencedStaleRetryRequest{
			Attempt:            attempt,
			ReplacementAgentID: replacementAgentID,
			HasReplacement:     hasReplacement,
			OccurredAt:         nowTime.UTC().Format(time.RFC3339),
			CurrentAttemptID:   currentAttemptID,
			CurrentEventSeq:    nextSeq,
			RetryDecision:      retryDecision,
		})
		if err != nil {
			if errors.Is(err, errStaleAttemptExecutionLeaseChanged) || errors.Is(err, errProviderTerminalHandoffPending) {
				return recoveryOutcome{}, nil
			}
			return recoveryOutcome{}, err
		}
		if queued.NodeRunID == "" {
			return recoveryOutcome{failed: true, agentBlocked: hasReplacement}, nil
		}

		return recoveryOutcome{reassigned: true, agentBlocked: hasReplacement}, nil
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
		"error":                   staleAttemptFailureReasonText,
		"node_run_id":             attempt.NodeRunID,
		"run_id":                  attempt.RunID,
		"node_id":                 attempt.NodeID,
		"candidate_started_at":    strings.TrimSpace(attempt.StartedAt),
		"evaluated_at":            nowTime.UTC().Format(time.RFC3339),
		"stale_threshold_seconds": int(DefaultReassignCandidateThreshold.Seconds()),
		"planning_grace_seconds":  int(defaultPlanningStaleGraceWindow().Seconds()),
		"reason":                  "stale_attempt",
		"retry_allowed":           plan.ShouldRetry && !meaningfulProgress,
		"meaningful_progress":     meaningfulProgress,
		"replacement_available":   hasReplacement,
		"retries_attempted":       retriesAttempted,
		"max_retries":             maxInt(0, plan.MaxRetries),
		"stale_duration_seconds":  staleSeconds,
		"reconciler_authority":    "system.reconciler",
		"same_agent_retry":        allowSelfRetry,
		"latest_progress":         progressDiagnostics,
	})
	if err != nil {
		return recoveryOutcome{}, fmt.Errorf("marshal stale attempt failure payload: %w", err)
	}

	terminalTransition := TransitionRequest{
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
	}
	if meaningfulProgress || plan.DeadLetter {
		decision := RetryDecisionExhausted
		if meaningfulProgress {
			decision = RetryDecisionFailFast
		}
		retryIndex := retriesAttempted
		if retryIndex > plan.MaxRetries {
			retryIndex = plan.MaxRetries
		}
		if err := r.transition.RecordDirectExecutionTerminalWithRetry(ctx, DirectExecutionTerminalWithRetryRequest{
			Transition:   terminalTransition,
			StaleAttempt: &attempt,
			Decision: RetryDecisionCreateRequest{
				DecisionID:         fmt.Sprintf("retry_decision_%s", strings.ReplaceAll(currentAttemptID, ":", "_")),
				SubjectKind:        RetrySubjectKindWorkOrderAttempt,
				RunID:              attempt.RunID,
				NodeID:             attempt.NodeID,
				NodeRunID:          attempt.NodeRunID,
				SourceAttemptID:    currentAttemptID,
				PolicyID:           retryPlannerPolicyID(r.retryPlanner),
				RetryClass:         plan.RetryClass,
				ErrorCode:          string(errorcatalog.CodeAgentUnresponsive),
				OperatorText:       automaticRetryDecisionText(decision, meaningfulProgress),
				MeaningfulProgress: meaningfulProgress,
				Decision:           decision,
				RetryIndex:         retryIndex,
				MaxRetries:         plan.MaxRetries,
				PolicyDelay:        plan.NextDelay,
				DueAt:              nowTime.UTC().Add(plan.NextDelay),
				CreatedAt:          nowTime.UTC(),
				EscalationTarget:   plan.EscalationTarget,
			},
			EscalationFromAgentID: attempt.AgentID,
		}); err != nil {
			if errors.Is(err, errStaleAttemptExecutionLeaseChanged) || errors.Is(err, errProviderTerminalHandoffPending) {
				return recoveryOutcome{}, nil
			}
			return recoveryOutcome{}, err
		}
	} else if err := r.transition.RecordDirectExecutionTerminal(ctx, DirectExecutionTerminalRequest{Transition: terminalTransition, StaleAttempt: &attempt}); err != nil {
		if errors.Is(err, errStaleAttemptExecutionLeaseChanged) || errors.Is(err, errProviderTerminalHandoffPending) {
			return recoveryOutcome{}, nil
		}
		return recoveryOutcome{}, err
	}
	if err := r.blockAgentIfNeeded(ctx, attempt.AgentID, nowTime); err != nil {
		return recoveryOutcome{}, err
	}

	return recoveryOutcome{failed: true, agentBlocked: true}, nil
}

// hasMeaningfulStaleAttemptProgress only admits durable provider-produced work.
// Runtime diagnostics and lifecycle heartbeats are intentionally excluded: they
// establish liveness, not work that makes an automatic retry unsafe.
func (r *Reconciler) hasMeaningfulStaleAttemptProgress(ctx context.Context, attemptID string) (bool, error) {
	if r == nil || r.db == nil || strings.TrimSpace(attemptID) == "" {
		return false, nil
	}

	rows, err := r.db.QueryContext(ctx, `SELECT event_type, reason_code, payload_json
		FROM events
		WHERE attempt_id = ?
		  AND event_type IN ('attempt.progress', 'attempt.activity')`, strings.TrimSpace(attemptID))
	if err != nil {
		return false, fmt.Errorf("load stale attempt progress evidence: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var eventType, reasonCode, payload string
		if err := rows.Scan(&eventType, &reasonCode, &payload); err != nil {
			return false, fmt.Errorf("scan stale attempt progress evidence: %w", err)
		}
		if staleAttemptEventShowsMeaningfulProgress(eventType, reasonCode, payload) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate stale attempt progress evidence: %w", err)
	}

	var tokenCount int
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(1)
		FROM token_usage
		WHERE attempt_id = ?
		  AND (input_tokens > 0 OR output_tokens > 0 OR total_tokens > 0 OR reasoning_tokens > 0 OR cache_read_tokens > 0 OR cache_write_tokens > 0)`, strings.TrimSpace(attemptID)).Scan(&tokenCount); err != nil {
		return false, fmt.Errorf("load stale attempt token evidence: %w", err)
	}
	if tokenCount > 0 {
		return true, nil
	}

	var workspaceDeltaCount int
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(1)
		FROM attempt_executions ae
		JOIN filesystem_snapshot_entries fse ON fse.snapshot_id = ae.result_snapshot_id
		WHERE ae.attempt_id = ?
		  AND TRIM(ae.base_snapshot_id) <> ''
		  AND TRIM(ae.result_snapshot_id) <> ''`, strings.TrimSpace(attemptID)).Scan(&workspaceDeltaCount); err != nil {
		return false, fmt.Errorf("load stale attempt workspace evidence: %w", err)
	}
	if workspaceDeltaCount > 0 {
		return true, nil
	}

	return r.providerEvidenceShowsMeaningfulProgress(ctx, attemptID)
}

func staleAttemptEventShowsMeaningfulProgress(eventType, reasonCode, payloadRaw string) bool {
	var payload map[string]any
	if json.Unmarshal([]byte(payloadRaw), &payload) != nil {
		return false
	}
	switch strings.TrimSpace(eventType) {
	case "attempt.progress":
		return strings.TrimSpace(stringValue(payload["text"])) != ""
	case AttemptActivityEventType:
		if strings.TrimSpace(reasonCode) != "provider_activity" {
			return false
		}
		switch strings.TrimSpace(stringValue(payload["kind"])) {
		case "kingdom.reasoning", "kingdom.tool":
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func (r *Reconciler) providerEvidenceShowsMeaningfulProgress(ctx context.Context, attemptID string) (bool, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT payload_json
		FROM provider_completion_evidence
		WHERE attempt_id = ? AND evidence_kind = 'stream_observation'
		UNION ALL
		SELECT stream_observation_json
		FROM provider_terminal_handoffs
		WHERE attempt_id = ?`, strings.TrimSpace(attemptID), strings.TrimSpace(attemptID))
	if err != nil {
		return false, fmt.Errorf("load stale provider stream evidence: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return false, fmt.Errorf("scan stale provider stream evidence: %w", err)
		}
		if providerStreamEvidenceShowsMeaningfulProgress(payload) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate stale provider stream evidence: %w", err)
	}
	return false, nil
}

func providerStreamEvidenceShowsMeaningfulProgress(payloadRaw string) bool {
	var envelope struct {
		Observation *providerexecution.InvocationStreamObservation `json:"observation"`
	}
	if json.Unmarshal([]byte(payloadRaw), &envelope) != nil {
		return false
	}
	observation := providerexecution.InvocationStreamObservation{}
	if envelope.Observation != nil {
		observation = *envelope.Observation
	} else {
		if json.Unmarshal([]byte(payloadRaw), &observation) != nil {
			return false
		}
	}
	return observation.MeaningfulProgress ||
		strings.TrimSpace(observation.CanonicalTerminalText) != "" ||
		len(observation.ToolCalls) > 0 ||
		observation.Tokens.InputTokens > 0 ||
		observation.Tokens.OutputTokens > 0 ||
		observation.Tokens.TotalTokens > 0 ||
		observation.Tokens.ReasoningTokens > 0 ||
		observation.Tokens.CacheReadTokens > 0 ||
		observation.Tokens.CacheWriteTokens > 0
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
	}
	var providerInvocationID string
	err = r.db.QueryRowContext(ctx, `SELECT pi.provider_invocation_id
		FROM provider_invocations pi
		WHERE pi.provider_invocation_id = (
			SELECT active_provider_invocation_id
			FROM attempt_executions
			WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?
		) LIMIT 1`, trimmedAttemptID, strings.TrimSpace(attempt.RunID), strings.TrimSpace(attempt.NodeRunID)).Scan(&providerInvocationID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load provider invocation state for stale recovery: %w", err)
	}
	var handoffPending int
	if err := r.db.QueryRowContext(ctx, `SELECT
		EXISTS(SELECT 1 FROM provider_terminal_handoffs WHERE provider_invocation_id = ?)
		OR EXISTS(SELECT 1 FROM provider_invocations WHERE provider_invocation_id = ? AND (terminal_handoff_pending_at IS NOT NULL OR terminal_recovery_required_at IS NOT NULL))`,
		providerInvocationID, providerInvocationID,
	).Scan(&handoffPending); err != nil {
		return false, fmt.Errorf("check provider terminal handoff before stale recovery: %w", err)
	}
	if handoffPending != 0 {
		// A pending handoff or durable pre-publication recovery marker carries
		// terminal process evidence that must settle before stale recovery can
		// revoke this invocation.
		return true, nil
	}
	return false, nil
}

func (r *Reconciler) convergeProviderSettledAttempts(ctx context.Context, nowTime time.Time) error {
	if r == nil || r.db == nil {
		return nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT ae.attempt_id,
	       nr.node_id
	FROM attempt_executions ae
	JOIN node_runs nr ON nr.node_run_id = ae.node_run_id AND nr.run_id = ae.run_id
	LEFT JOIN provider_completion_settlements pcs ON pcs.provider_invocation_id = ae.active_provider_invocation_id
	WHERE ae.execution_mode = 'provider_backed'
	  AND ae.state NOT IN ('committed_success', 'committed_failure', 'committed_cancelled')
	  AND COALESCE(TRIM(ae.active_provider_invocation_id), '') <> ''
	  AND pcs.settlement_id IS NOT NULL
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

	settlement, foundSettlement, err := store.GetProviderCompletionSettlementByInvocation(ctx, providerInvocationID)
	if err != nil {
		return false, fmt.Errorf("load provider settlement for stale recovery attempt %q: %w", trimmedAttemptID, err)
	}
	if !foundSettlement {
		return false, nil
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
	return true, nil
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
	cutoff := nowTime.Add(-threshold).UTC()
	query := fmt.Sprintf(`SELECT n.node_run_id,
		        n.run_id,
		        n.node_id,
		        COALESCE(wn.node_class, ''),
		        n.attempt,
		        n.wave,
		        n.required_role,
		        n.agent_id,
		        COALESCE(ae.session_ref, ''),
		        COALESCE(ae.state, ''),
	        COALESCE(ae.execution_mode, 'runtime_owned'),
	        COALESCE(ae.lease_owner, ''),
	        COALESCE(ae.lease_version, 0),
	        COALESCE(ae.lease_expires_at, ''),
		        n.node_status,
		        CASE
		          WHEN n.node_status = 'starting' THEN COALESCE(ae.launch_started_at, '')
		          ELSE COALESCE(n.started_at, '')
		        END AS candidate_started_at
		 FROM node_runs n
		 JOIN workflow_runs wr ON wr.run_id = n.run_id
		 LEFT JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = n.node_id
		 JOIN agents a ON a.agent_id = n.agent_id
		 LEFT JOIN attempt_executions ae ON ae.attempt_id = n.run_id || ':' || n.node_id || ':' || n.attempt
		 LEFT JOIN session_leases sl ON sl.session_id = ae.session_ref AND sl.status = 'active'
		 WHERE n.node_status IN ('starting', 'running')
		   AND (
		     (
		       n.node_status = 'starting'
		       AND COALESCE(ae.state, '') = 'starting'
		       AND COALESCE(ae.launch_started_at, '') != ''
		       AND (
		         sl.session_id IS NULL OR
		         %s
		       )
		       AND (
		         COALESCE(ae.execution_mode, 'runtime_owned') <> 'provider_backed' OR
		         (COALESCE(TRIM(ae.lease_owner), '') = '' AND COALESCE(TRIM(ae.lease_expires_at), '') = '') OR
		         (COALESCE(TRIM(ae.lease_owner), '') <> '' AND COALESCE(TRIM(ae.lease_expires_at), '') <> '' AND %s)
		       )
		     ) OR (
		       n.node_status = 'running'
		       AND n.started_at IS NOT NULL
		       AND COALESCE(ae.state, '') NOT IN ('starting', 'terminal_observed', 'committed_success', 'committed_failure', 'committed_cancelled')
		       AND (
		         COALESCE(ae.execution_mode, 'runtime_owned') <> 'provider_backed' OR
		         (COALESCE(TRIM(ae.lease_owner), '') = '' AND COALESCE(TRIM(ae.lease_expires_at), '') = '') OR
		         (COALESCE(TRIM(ae.lease_owner), '') <> '' AND COALESCE(TRIM(ae.lease_expires_at), '') <> '' AND %s)
		       )
		     )
		   )
		ORDER BY n.run_id ASC, n.node_id ASC, n.attempt ASC`,
		staleActiveSessionLeaseConditionSQL("sl"),
		timestamporder.ComparisonSQL("ae.lease_expires_at", "?", "<="),
		timestamporder.ComparisonSQL("ae.lease_expires_at", "?", "<="),
	)

	rows, err := q.QueryContext(ctx,
		query,
		cutoff.Format(time.RFC3339Nano),
		nowTime.UTC().Format(time.RFC3339Nano),
		nowTime.UTC().Format(time.RFC3339Nano),
		nowTime.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return nil, fmt.Errorf("query stale attempts: %w", err)
	}
	defer rows.Close()

	candidates := make([]staleAttempt, 0)
	for rows.Next() {
		var candidate staleAttempt
		var executionMode string
		if err := rows.Scan(
			&candidate.NodeRunID,
			&candidate.RunID,
			&candidate.NodeID,
			&candidate.NodeClass,
			&candidate.Attempt,
			&candidate.Wave,
			&candidate.RequiredRole,
			&candidate.AgentID,
			&candidate.SessionRef,
			&candidate.AttemptState,
			&executionMode,
			&candidate.LeaseOwner,
			&candidate.LeaseVersion,
			&candidate.LeaseExpiresAt,
			&candidate.CurrentStatus,
			&candidate.StartedAt,
		); err != nil {
			return nil, fmt.Errorf("scan stale attempt: %w", err)
		}
		candidate.ExecutionMode = executionstore.NormalizeExecutionMode(executionMode)
		candidates = append(candidates, candidate)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stale attempts: %w", err)
	}

	stale := make([]staleAttempt, 0, len(candidates))
	for _, candidate := range candidates {
		startedAt, parseErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(candidate.StartedAt))
		if parseErr != nil || startedAt.UTC().After(cutoff) {
			continue
		}
		if isPlanningNodeClass(workflow.ResolveNodeClass(candidate.NodeID, candidate.NodeClass)) {
			planningCutoff := nowTime.Add(-(threshold + defaultPlanningStaleGraceWindow())).UTC()
			if startedAt.UTC().After(planningCutoff) {
				continue
			}
		}
		grace := threshold
		if grace < defaultProgressStaleGrace {
			grace = defaultProgressStaleGrace
		}
		skip, err := HasRecentExecutionLiveness(ctx, q, candidate.NodeRunID, nowTime, grace)
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

func isPlanningNodeClass(nodeClass string) bool {
	switch nodeClass {
	case workflow.NodeClassWorkIntake, workflow.NodeClassAdvisorConsultation, workflow.NodeClassAdvisorySynthesis, workflow.NodeClassPlanDrafting, workflow.NodeClassPlanDecision:
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

func normalizeReconcileInputs(req ReconcileRequest, nowFn func() time.Time) (time.Time, time.Duration, time.Duration, error) {
	var nowTime time.Time
	if strings.TrimSpace(req.Now) == "" {
		nowTime = nowFn().UTC()
	} else {
		parsed, err := time.Parse(time.RFC3339Nano, req.Now)
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

func composeAttemptID(runID, nodeID string, attempt int) string {
	return fmt.Sprintf("%s:%s:%d", runID, nodeID, attempt)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
