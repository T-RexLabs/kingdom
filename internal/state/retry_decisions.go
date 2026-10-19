package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type RetrySubjectKind string

const (
	RetrySubjectKindWorkOrderAttempt   RetrySubjectKind = "work_order_attempt"
	RetrySubjectKindPlanningInvocation RetrySubjectKind = "planning_invocation"
)

type RetryDecisionKind string

const (
	RetryDecisionRetry     RetryDecisionKind = "retry"
	RetryDecisionFailFast  RetryDecisionKind = "fail_fast"
	RetryDecisionExhausted RetryDecisionKind = "exhausted"
)

type RetryDispatchState string

const (
	RetryDispatchStatePending       RetryDispatchState = "pending"
	RetryDispatchStateDispatched    RetryDispatchState = "dispatched"
	RetryDispatchStateNotApplicable RetryDispatchState = "not_applicable"
)

type RetryDecisionResolution string

const (
	RetryDecisionResolutionPending       RetryDecisionResolution = "pending"
	RetryDecisionResolutionRecovered     RetryDecisionResolution = "recovered"
	RetryDecisionResolutionFailed        RetryDecisionResolution = "failed"
	RetryDecisionResolutionEscalated     RetryDecisionResolution = "escalated"
	RetryDecisionResolutionNotApplicable RetryDecisionResolution = "not_applicable"
	RetryDecisionResolutionCancelled     RetryDecisionResolution = "cancelled"
)

type RetryDecision struct {
	DecisionID                 string
	SubjectKind                RetrySubjectKind
	RunID                      string
	NodeID                     string
	NodeRunID                  string
	PlanningOperationID        string
	SourceAttemptID            string
	SourcePlanningInvocationID string
	TargetNodeRunID            string
	PolicyID                   string
	RetryClass                 string
	ErrorCode                  string
	OperatorText               string
	MeaningfulProgress         bool
	Decision                   RetryDecisionKind
	RetryIndex                 int
	MaxRetries                 int
	PolicyDelay                time.Duration
	DueAt                      time.Time
	DispatchState              RetryDispatchState
	DispatchedAt               *time.Time
	EscalationTarget           string
	Resolution                 RetryDecisionResolution
	ResolvedAt                 *time.Time
	CreatedAt                  time.Time
}

type RetryDecisionCreateRequest struct {
	DecisionID                 string
	SubjectKind                RetrySubjectKind
	RunID                      string
	NodeID                     string
	NodeRunID                  string
	PlanningOperationID        string
	SourceAttemptID            string
	SourcePlanningInvocationID string
	TargetNodeRunID            string
	PolicyID                   string
	RetryClass                 string
	ErrorCode                  string
	OperatorText               string
	MeaningfulProgress         bool
	Decision                   RetryDecisionKind
	RetryIndex                 int
	MaxRetries                 int
	PolicyDelay                time.Duration
	DueAt                      time.Time
	CreatedAt                  time.Time
	EscalationTarget           string
	Resolution                 RetryDecisionResolution
	ResolvedAt                 *time.Time
}

func normalizeRetryDecisionCreateRequest(req RetryDecisionCreateRequest) (RetryDecision, error) {
	decision := RetryDecision{
		DecisionID:                 strings.TrimSpace(req.DecisionID),
		SubjectKind:                req.SubjectKind,
		RunID:                      strings.TrimSpace(req.RunID),
		NodeID:                     strings.TrimSpace(req.NodeID),
		NodeRunID:                  strings.TrimSpace(req.NodeRunID),
		PlanningOperationID:        strings.TrimSpace(req.PlanningOperationID),
		SourceAttemptID:            strings.TrimSpace(req.SourceAttemptID),
		SourcePlanningInvocationID: strings.TrimSpace(req.SourcePlanningInvocationID),
		TargetNodeRunID:            strings.TrimSpace(req.TargetNodeRunID),
		PolicyID:                   strings.TrimSpace(req.PolicyID),
		RetryClass:                 strings.TrimSpace(req.RetryClass),
		ErrorCode:                  strings.TrimSpace(req.ErrorCode),
		OperatorText:               strings.TrimSpace(req.OperatorText),
		MeaningfulProgress:         req.MeaningfulProgress,
		Decision:                   req.Decision,
		RetryIndex:                 req.RetryIndex,
		MaxRetries:                 req.MaxRetries,
		PolicyDelay:                req.PolicyDelay,
		DueAt:                      req.DueAt.UTC(),
		EscalationTarget:           strings.TrimSpace(req.EscalationTarget),
		Resolution:                 RetryDecisionResolution(strings.TrimSpace(string(req.Resolution))),
		ResolvedAt:                 normalizeRetryTime(req.ResolvedAt),
		CreatedAt:                  req.CreatedAt.UTC(),
	}
	if decision.DecisionID == "" || decision.PolicyID == "" || decision.RetryClass == "" || decision.ErrorCode == "" || decision.EscalationTarget == "" {
		return RetryDecision{}, fmt.Errorf("decision_id, policy_id, retry_class, error_code, and escalation_target are required")
	}
	if decision.OperatorText == "" {
		return RetryDecision{}, fmt.Errorf("operator_text is required")
	}
	if decision.RetryIndex < 0 || decision.MaxRetries < 0 || decision.PolicyDelay < 0 {
		return RetryDecision{}, fmt.Errorf("retry_index, max_retries, and policy_delay must be non-negative")
	}
	if decision.RetryIndex > decision.MaxRetries {
		return RetryDecision{}, fmt.Errorf("retry_index must not exceed max_retries")
	}
	if decision.DueAt.IsZero() || decision.CreatedAt.IsZero() {
		return RetryDecision{}, fmt.Errorf("due_at and created_at are required")
	}
	if decision.DueAt.Before(decision.CreatedAt.Add(decision.PolicyDelay)) {
		return RetryDecision{}, fmt.Errorf("due_at must not precede created_at plus policy_delay")
	}
	switch decision.SubjectKind {
	case RetrySubjectKindWorkOrderAttempt:
		if decision.RunID == "" || decision.NodeID == "" || decision.NodeRunID == "" || decision.SourceAttemptID == "" || decision.PlanningOperationID != "" || decision.SourcePlanningInvocationID != "" {
			return RetryDecision{}, fmt.Errorf("work order attempt retry decisions require run_id, node_id, node_run_id, and source_attempt_id")
		}
	case RetrySubjectKindPlanningInvocation:
		if decision.PlanningOperationID == "" || decision.SourcePlanningInvocationID == "" || decision.RunID != "" || decision.NodeID != "" || decision.NodeRunID != "" || decision.SourceAttemptID != "" || decision.TargetNodeRunID != "" {
			return RetryDecision{}, fmt.Errorf("planning invocation retry decisions require only planning_operation_id and source_planning_invocation_id")
		}
	default:
		return RetryDecision{}, fmt.Errorf("invalid retry decision subject kind %q", decision.SubjectKind)
	}
	switch decision.Decision {
	case RetryDecisionRetry:
		if decision.SubjectKind == RetrySubjectKindWorkOrderAttempt && decision.TargetNodeRunID == "" {
			return RetryDecision{}, fmt.Errorf("work order retry decisions require target_node_run_id")
		}
		decision.DispatchState = RetryDispatchStatePending
		decision.Resolution = RetryDecisionResolutionPending
		decision.ResolvedAt = nil
	case RetryDecisionFailFast, RetryDecisionExhausted:
		if decision.TargetNodeRunID != "" {
			return RetryDecision{}, fmt.Errorf("%s retry decision must not have target_node_run_id", decision.Decision)
		}
		decision.DispatchState = RetryDispatchStateNotApplicable
		if decision.Decision == RetryDecisionExhausted {
			decision.Resolution = RetryDecisionResolutionEscalated
			decision.ResolvedAt = &decision.CreatedAt
		} else {
			decision.Resolution = RetryDecisionResolutionNotApplicable
			decision.ResolvedAt = nil
		}
	default:
		return RetryDecision{}, fmt.Errorf("invalid retry decision %q", decision.Decision)
	}
	return decision, nil
}

func insertRetryDecisionTx(ctx context.Context, tx *sql.Tx, req RetryDecisionCreateRequest) (RetryDecision, error) {
	decision, err := normalizeRetryDecisionCreateRequest(req)
	if err != nil {
		return RetryDecision{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO retry_decisions (
		retry_decision_id, subject_kind, run_id, node_id, node_run_id, planning_operation_id, source_attempt_id, source_planning_invocation_id, target_node_run_id,
		policy_id, retry_class, error_code, operator_text, meaningful_progress, decision, retry_index, max_retries, policy_delay_ns,
		due_at, dispatch_state, dispatched_at, escalation_target, resolution, resolved_at, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		decision.DecisionID, decision.SubjectKind, nullableRetryString(decision.RunID), nullableRetryString(decision.NodeID), nullableRetryString(decision.NodeRunID), nullableRetryString(decision.PlanningOperationID), nullableRetryString(decision.SourceAttemptID), nullableRetryString(decision.SourcePlanningInvocationID), nullableRetryString(decision.TargetNodeRunID),
		decision.PolicyID, decision.RetryClass, decision.ErrorCode, decision.OperatorText, boolToSQLite(decision.MeaningfulProgress), decision.Decision, decision.RetryIndex, decision.MaxRetries, decision.PolicyDelay.Nanoseconds(),
		retryTimeString(decision.DueAt), decision.DispatchState, nullableRetryTime(decision.DispatchedAt), decision.EscalationTarget, decision.Resolution, nullableRetryTime(decision.ResolvedAt), retryTimeString(decision.CreatedAt),
	)
	if err != nil {
		return RetryDecision{}, fmt.Errorf("insert retry decision: %w", err)
	}
	return decision, nil
}

func normalizeRetryTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.UTC()
	return &normalized
}

func retryTimeString(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func nullableRetryTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return retryTimeString(*value)
}

func parseRetryTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse retry decision timestamp %q: %w", value, err)
	}
	return parsed.UTC(), nil
}

func boolToSQLite(value bool) int {
	if value {
		return 1
	}
	return 0
}

func nullableRetryString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
