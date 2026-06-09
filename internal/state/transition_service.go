// Package state owns Kingdom's SQLite-backed source of truth.
//
// It enforces legal transitions, persists the append-only audit and execution
// event streams, and provides the canonical mutation and query services for
// runs, nodes, planning sessions, leases, artifacts, mailbox traffic, and
// related operational metadata.
package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/core/rolecomm"
	"kingdom/internal/core/schemas"
	"kingdom/internal/core/workflow"
)

type EntityType string

const (
	EntityTypeRun  EntityType = "run"
	EntityTypeNode EntityType = "node"
	EntityTypePlan EntityType = "plan"
)

type IllegalTransitionError struct {
	EntityType EntityType
	FromState  string
	ToState    string
}

func (e *IllegalTransitionError) Error() string {
	return fmt.Sprintf("illegal %s transition: %q -> %q", e.EntityType, e.FromState, e.ToState)
}

type StateConflictError struct {
	EntityType    EntityType
	ExpectedState string
	ActualState   string
}

func (e *StateConflictError) Error() string {
	return fmt.Sprintf("%s state conflict: expected %q got %q", e.EntityType, e.ExpectedState, e.ActualState)
}

type ReplayWaveActiveError struct {
	RunID string
	Scope string
}

func (e *ReplayWaveActiveError) Error() string {
	scope := strings.TrimSpace(e.Scope)
	if scope == "" {
		scope = "replay wave"
	}
	return fmt.Sprintf("%s already active for run %q", scope, e.RunID)
}

type EntityNotFoundError struct {
	EntityType EntityType
	RunID      string
	NodeRunID  string
}

func (e *EntityNotFoundError) Error() string {
	if e.EntityType == EntityTypeNode {
		return fmt.Sprintf("%s entity not found for node_run_id=%q", e.EntityType, e.NodeRunID)
	}
	return fmt.Sprintf("%s entity not found for run_id=%q", e.EntityType, e.RunID)
}

type TransitionRequest struct {
	TransitionID string
	EventID      string

	RunID     string
	NodeRunID sql.NullString
	Entity    EntityType
	FromState string
	ToState   string

	ActorID         string
	RoleID          string
	AuthoritySource string
	ReasonCode      string
	ReasonText      string
	CorrelationID   string
	CausationID     string
	OccurredAt      string

	TransitionPrevHash  string
	TransitionEntryHash string
	EventPrevHash       string
	EventEntryHash      string

	AttemptID     string
	SchemaVersion string
	EventType     string
	RunEventSeq   int
	EventSeq      int
	PayloadJSON   string

	SkipRunCollectionProjection bool
}

type TransitionService struct {
	db                      *sql.DB
	eventSchemas            *schemas.Set
	roleCommunicationPolicy rolecomm.Policy
}

type transitionApplyOptions struct {
	LoopCapParticipantNodeIDs []string
}

const (
	runTerminalizationAuthoritySource       = "state.run_terminalization"
	durableAttemptProjectionAuthoritySource = "state.durable_attempt_projection"
)

func NewTransitionService(db *sql.DB, eventSchemas *schemas.Set) *TransitionService {
	return &TransitionService{db: db, eventSchemas: eventSchemas}
}

func (s *TransitionService) WithRoleCommunicationPolicy(policy rolecomm.Policy) *TransitionService {
	if s == nil {
		return nil
	}
	s.roleCommunicationPolicy = policy
	return s
}

var legalRunTransitions = map[string]map[string]struct{}{
	"pending": {
		"running": {},
	},
	"running": {
		"awaiting_human_confirmation": {},
		"succeeded":                   {},
		"failed":                      {},
		"cancelled":                   {},
	},
	"awaiting_human_confirmation": {
		"running":   {},
		"succeeded": {},
		"failed":    {},
		"cancelled": {},
	},
	"failed": {
		"running": {},
	},
}

var legalNodeTransitions = map[string]map[string]struct{}{
	"pending": {
		"ready":     {},
		"cancelled": {},
	},
	"ready": {
		"starting":  {},
		"running":   {},
		"succeeded": {},
		"failed":    {},
		"cancelled": {},
	},
	"starting": {
		"blocked":   {},
		"running":   {},
		"failed":    {},
		"cancelled": {},
	},
	"running": {
		"succeeded": {},
		"failed":    {},
		"blocked":   {},
		"cancelled": {},
		"skipped":   {},
	},
	"blocked": {
		"cancelled": {},
	},
	"failed": {
		"succeeded": {},
	},
}

var legalPlanTransitions = map[string]map[string]struct{}{
	"draft": {
		"in_council_review": {},
	},
	"in_council_review": {
		"awaiting_king_decision": {},
	},
	"awaiting_king_decision": {
		"approved":     {},
		"needs_rework": {},
	},
	"needs_rework": {
		"in_council_review": {},
	},
	"approved": {
		"needs_rework": {},
	},
}

var executionGuardNodeClasses = map[string]struct{}{
	"execution_kickoff":   {},
	"delegated_execution": {},
	"execution_review":    {},
	"validation_gates":    {},
	"royal_closure":       {},
}

var governancePredecessorNodeClasses = []string{
	"decree_intake",
	"advisor_consultation",
	"advisory_synthesis",
	"plan_drafting",
	"king_plan_decision",
}

func (s *TransitionService) Apply(ctx context.Context, req TransitionRequest) error {
	return s.applyWithEffects(ctx, req, nil)
}

type transitionEffectFunc func(context.Context, *sql.Tx) error

func (s *TransitionService) applyWithEffects(ctx context.Context, req TransitionRequest, effects transitionEffectFunc) error {
	return s.applyWithEffectsWithOptions(ctx, req, effects, transitionApplyOptions{})
}

func (s *TransitionService) applyWithEffectsWithOptions(ctx context.Context, req TransitionRequest, effects transitionEffectFunc, opts transitionApplyOptions) error {
	return retrySQLiteBusy(ctx, func() error {
		return s.applyWithEffectsWithOptionsOnce(ctx, req, effects, opts)
	})
}

func (s *TransitionService) applyWithEffectsWithOptionsOnce(ctx context.Context, req TransitionRequest, effects transitionEffectFunc, opts transitionApplyOptions) error {
	if !bypassesTransitionValidation(req) {
		if err := validateTransition(req.Entity, req.FromState, req.ToState); err != nil {
			return err
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transition transaction: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := s.applyTxWithOptions(ctx, tx, req, opts); err != nil {
		return err
	}

	if effects != nil {
		if err := effects(ctx, tx); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transition transaction: %w", err)
	}
	committed = true
	return nil
}

func (s *TransitionService) applyTx(ctx context.Context, tx *sql.Tx, req TransitionRequest) error {
	return s.applyTxWithOptions(ctx, tx, req, transitionApplyOptions{})
}

func (s *TransitionService) applyTxWithOptions(ctx context.Context, tx *sql.Tx, req TransitionRequest, opts transitionApplyOptions) error {
	if err := s.enforceTransitionInvariants(ctx, tx, req, opts); err != nil {
		return err
	}

	if err := s.applyEntityState(ctx, tx, req); err != nil {
		return err
	}
	if req.RunEventSeq <= 0 {
		runEventSeq, err := nextRunEventSeqTx(ctx, tx, req.RunID)
		if err != nil {
			return fmt.Errorf("allocate run event sequence: %w", err)
		}
		req.RunEventSeq = runEventSeq
	}
	_, err := AdvanceRunActivityTx(ctx, tx, RunActivityUpdateRequest{
		EventID:     req.EventID,
		EventType:   req.EventType,
		OccurredAt:  req.OccurredAt,
		PayloadJSON: req.PayloadJSON,
		ReasonCode:  req.ReasonCode,
		RunEventSeq: req.RunEventSeq,
		RunID:       req.RunID,
	})
	if err != nil {
		return fmt.Errorf("advance run activity: %w", err)
	}

	if err := s.prepareEventPayloadTx(ctx, tx, &req); err != nil {
		return fmt.Errorf("prepare event payload: %w", err)
	}

	if err := assignTransitionHashes(ctx, tx, &req); err != nil {
		return fmt.Errorf("assign transition audit hashes: %w", err)
	}
	if err := validateTransitionEventContract(s.eventSchemas, req); err != nil {
		return err
	}
	if err := assignTransitionEventHashes(ctx, tx, &req); err != nil {
		return fmt.Errorf("assign transition event audit hashes: %w", err)
	}

	if err := s.insertTransition(ctx, tx, req); err != nil {
		return fmt.Errorf("insert transition: %w", err)
	}

	if err := s.insertEvent(ctx, tx, req); err != nil {
		return fmt.Errorf("insert event: %w", err)
	}

	return nil
}

func (s *TransitionService) prepareEventPayloadTx(ctx context.Context, tx *sql.Tx, req *TransitionRequest) error {
	if req == nil || req.EventType != "run.state_changed" {
		return nil
	}

	projectedPayload, err := projectRunStateChangedPayloadTx(ctx, tx, req.RunID, req.PayloadJSON)
	if err != nil {
		return err
	}
	req.PayloadJSON = projectedPayload
	return nil
}

func (s *TransitionService) enforceTransitionInvariants(ctx context.Context, tx *sql.Tx, req TransitionRequest, opts transitionApplyOptions) error {
	if req.Entity == EntityTypeNode {
		if bypassesNodeTransitionRunStatusGate(req) {
			return nil
		}
		allowed, err := s.nodeTransitionAllowedForRunStatus(ctx, tx, req)
		if err != nil {
			return err
		}
		if !allowed {
			return &IllegalTransitionError{EntityType: req.Entity, FromState: req.FromState, ToState: req.ToState}
		}
		if req.ToState == "succeeded" {
			nodeID, err := s.nodeIDForNodeRun(ctx, tx, req.NodeRunID.String)
			if err != nil {
				return err
			}
			nodeClass, err := s.nodeClassForNodeID(ctx, tx, req.RunID, nodeID)
			if err != nil {
				return err
			}
			trimmedNodeClass := strings.TrimSpace(nodeClass)
			if workflow.IsLoopCapAutoSuccessReasonCode(trimmedNodeClass, req.ReasonCode) {
				allowed, err := s.loopCapAutoSuccessAllowed(ctx, tx, req, trimmedNodeClass, opts)
				if err != nil {
					return err
				}
				if !allowed {
					return &IllegalTransitionError{EntityType: req.Entity, FromState: req.FromState, ToState: req.ToState}
				}
				return nil
			}
			if req.FromState != "running" {
				return &IllegalTransitionError{EntityType: req.Entity, FromState: req.FromState, ToState: req.ToState}
			}
		}
	}

	if req.Entity != EntityTypeNode || (req.ToState != "starting" && req.ToState != "running") || !req.NodeRunID.Valid {
		return nil
	}

	nodeID, err := s.nodeIDForNodeRun(ctx, tx, req.NodeRunID.String)
	if err != nil {
		return err
	}
	nodeClass, err := s.nodeClassForNodeID(ctx, tx, req.RunID, nodeID)
	if err != nil {
		return err
	}

	if _, guarded := executionGuardNodeClasses[nodeClass]; !guarded {
		return nil
	}

	approved, err := s.runPlanApproved(ctx, tx, req.RunID)
	if err != nil {
		return err
	}
	if !approved {
		return &IllegalTransitionError{EntityType: req.Entity, FromState: req.FromState, ToState: req.ToState}
	}

	for _, predecessorClass := range governancePredecessorNodeClasses {
		present, err := s.runIncludesNodeClass(ctx, tx, req.RunID, predecessorClass)
		if err != nil {
			return err
		}
		if !present {
			continue
		}

		succeeded, err := s.nodeClassSucceeded(ctx, tx, req.RunID, predecessorClass)
		if err != nil {
			return err
		}
		if !succeeded {
			return &IllegalTransitionError{EntityType: req.Entity, FromState: req.FromState, ToState: req.ToState}
		}
	}

	if nodeClass == "royal_closure" {
		validationExists, err := s.runIncludesNodeClass(ctx, tx, req.RunID, "validation_gates")
		if err != nil {
			return err
		}
		if !validationExists {
			return nil
		}

		validationPassed, err := s.nodeClassSucceeded(ctx, tx, req.RunID, "validation_gates")
		if err != nil {
			return err
		}
		if validationPassed {
			return nil
		}
		return &IllegalTransitionError{EntityType: req.Entity, FromState: req.FromState, ToState: req.ToState}
	}

	return nil
}

func bypassesNodeTransitionRunStatusGate(req TransitionRequest) bool {
	if req.Entity != EntityTypeNode {
		return false
	}
	switch strings.TrimSpace(req.AuthoritySource) {
	case runTerminalizationAuthoritySource, durableAttemptProjectionAuthoritySource:
		return true
	default:
		return false
	}
}

func bypassesTransitionValidation(req TransitionRequest) bool {
	return req.Entity == EntityTypeNode && strings.TrimSpace(req.AuthoritySource) == durableAttemptProjectionAuthoritySource
}

func (s *TransitionService) loopCapAutoSuccessAllowed(ctx context.Context, tx *sql.Tx, req TransitionRequest, nodeClass string, opts transitionApplyOptions) (bool, error) {
	if req.Entity != EntityTypeNode || !req.NodeRunID.Valid || req.ToState != "succeeded" {
		return false, nil
	}
	trimmedNodeClass := strings.TrimSpace(nodeClass)
	if !workflow.IsLoopCapAutoSuccessReasonCode(trimmedNodeClass, req.ReasonCode) {
		return false, nil
	}
	settings, err := loadRunLoopSettingsTx(ctx, tx, req.RunID)
	if err != nil {
		return false, err
	}
	if req.FromState != "ready" {
		return false, nil
	}
	nodeID, err := s.nodeIDForNodeRun(ctx, tx, req.NodeRunID.String)
	if err != nil {
		return false, err
	}
	participantNodeIDs := normalizeLoopCapParticipantNodeIDs(opts.LoopCapParticipantNodeIDs)
	if len(participantNodeIDs) == 0 {
		return false, fmt.Errorf("loop cap auto-success participant node ids are required")
	}
	if !containsLoopCapParticipantNode(participantNodeIDs, nodeID) {
		return false, fmt.Errorf("loop cap auto-success participant node ids must include node %q", nodeID)
	}
	wave, err := nodeWaveForNodeRunTx(ctx, tx, req.NodeRunID.String)
	if err != nil {
		return false, err
	}
	failedReviewCycles := 0
	failedValidationWaves := 0
	if settings.HasExecutionReview && trimmedNodeClass == workflow.NodeClassExecutionReview {
		failedReviewCycles, err = CountReviewReworkWavesBeforeWaveForNodeIDs(ctx, tx, req.RunID, participantNodeIDs, wave)
		if err != nil {
			return false, err
		}
	}
	if settings.HasValidationGates && trimmedNodeClass == workflow.NodeClassValidationGates {
		failedValidationWaves, err = CountValidationReworkWavesBeforeWaveForNodeIDs(ctx, tx, req.RunID, participantNodeIDs, wave)
		if err != nil {
			return false, err
		}
	}
	return workflow.ReadyNodeAutoSucceedsAtLoopCap(settings, trimmedNodeClass, wave, failedReviewCycles, failedValidationWaves), nil
}

func containsLoopCapParticipantNode(nodeIDs []string, nodeID string) bool {
	trimmedNodeID := strings.TrimSpace(nodeID)
	for _, candidate := range nodeIDs {
		if candidate == trimmedNodeID {
			return true
		}
	}
	return false
}

func (s *TransitionService) nodeTransitionAllowedForRunStatus(ctx context.Context, tx *sql.Tx, req TransitionRequest) (bool, error) {
	if req.Entity != EntityTypeNode {
		return true, nil
	}
	if strings.TrimSpace(req.AuthoritySource) == runTerminalizationAuthoritySource {
		return true, nil
	}

	var runStatus string
	err := tx.QueryRowContext(ctx, `SELECT run_status FROM workflow_runs WHERE run_id = ?`, req.RunID).Scan(&runStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return false, &EntityNotFoundError{EntityType: EntityTypeRun, RunID: req.RunID}
	}
	if err != nil {
		return false, fmt.Errorf("load run status for node transition: %w", err)
	}
	if runStatus == "running" {
		return true, nil
	}
	return false, nil
}

func (s *TransitionService) runIncludesNodeClass(ctx context.Context, tx *sql.Tx, runID, nodeClass string) (bool, error) {
	var count int
	err := tx.QueryRowContext(ctx,
		`SELECT COUNT(1)
		 FROM workflow_nodes wn
		 JOIN workflow_runs wr ON wr.workflow_definition_id = wn.workflow_definition_id
		 WHERE wr.run_id = ? AND wn.node_class = ?`,
		runID,
		nodeClass,
	).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("check workflow node class presence run=%q class=%q: %w", runID, nodeClass, err)
	}

	return count > 0, nil
}

func (s *TransitionService) nodeIDForNodeRun(ctx context.Context, tx *sql.Tx, nodeRunID string) (string, error) {
	var nodeID string
	err := tx.QueryRowContext(ctx, `SELECT node_id FROM node_runs WHERE node_run_id = ?`, nodeRunID).Scan(&nodeID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", &EntityNotFoundError{EntityType: EntityTypeNode, NodeRunID: nodeRunID}
	}
	if err != nil {
		return "", fmt.Errorf("load node id for node_run=%q: %w", nodeRunID, err)
	}
	return nodeID, nil
}

func (s *TransitionService) runPlanApproved(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var planState string
	err := tx.QueryRowContext(ctx, `SELECT plan_state FROM workflow_runs WHERE run_id = ?`, runID).Scan(&planState)
	if errors.Is(err, sql.ErrNoRows) {
		return false, &EntityNotFoundError{EntityType: EntityTypeRun, RunID: runID}
	}
	if err != nil {
		return false, fmt.Errorf("load plan state for run %q: %w", runID, err)
	}
	return planState == "approved", nil
}

func (s *TransitionService) nodeClassSucceeded(ctx context.Context, tx *sql.Tx, runID, nodeClass string) (bool, error) {
	var total int
	err := tx.QueryRowContext(ctx,
		`SELECT COUNT(1)
		 FROM workflow_nodes wn
		 JOIN workflow_runs wr ON wr.workflow_definition_id = wn.workflow_definition_id
		 WHERE wr.run_id = ? AND wn.node_class = ?`,
		runID,
		nodeClass,
	).Scan(&total)
	if err != nil {
		return false, fmt.Errorf("count node class members run=%q class=%q: %w", runID, nodeClass, err)
	}
	if total == 0 {
		return true, nil
	}

	var succeededLatest int
	err = tx.QueryRowContext(ctx,
		`WITH latest_attempts AS (
			SELECT nr.node_id,
			       nr.node_status,
			       ROW_NUMBER() OVER (
			         PARTITION BY nr.node_id
			         ORDER BY CASE
			           WHEN COALESCE(TRIM(nr.superseded_at), '') = '' THEN 0
			           ELSE 1
			         END ASC,
			         nr.attempt DESC,
			         nr.node_run_id DESC
			       ) AS rn
			FROM node_runs nr
			JOIN workflow_runs wr ON wr.run_id = nr.run_id
			JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
			WHERE nr.run_id = ? AND wn.node_class = ?
		)
		SELECT COUNT(1)
		FROM latest_attempts
		WHERE rn = 1 AND node_status = 'succeeded'`,
		runID,
		nodeClass,
	).Scan(&succeededLatest)
	if err != nil {
		return false, fmt.Errorf("check latest node class success run=%q class=%q: %w", runID, nodeClass, err)
	}

	return succeededLatest == total, nil
}

func (s *TransitionService) nodeClassForNodeID(ctx context.Context, tx *sql.Tx, runID, nodeID string) (string, error) {
	var nodeClass string
	err := tx.QueryRowContext(ctx,
		`SELECT wn.node_class
		 FROM workflow_nodes wn
		 JOIN workflow_runs wr ON wr.workflow_definition_id = wn.workflow_definition_id
		 WHERE wr.run_id = ? AND wn.node_id = ?
		 LIMIT 1`,
		runID,
		nodeID,
	).Scan(&nodeClass)
	if errors.Is(err, sql.ErrNoRows) {
		return nodeID, nil
	}
	if err != nil {
		return "", fmt.Errorf("load node class run=%q node=%q: %w", runID, nodeID, err)
	}
	return nodeClass, nil
}

func reviewReworkCycleCountBeforeWaveTx(ctx context.Context, tx *sql.Tx, runID string, wave int) (int, error) {
	return CountReviewReworkWavesBeforeWave(ctx, tx, runID, wave)
}
func validationReworkWaveCountBeforeWaveTx(ctx context.Context, tx *sql.Tx, runID string, wave int) (int, error) {
	return CountValidationReworkWavesBeforeWave(ctx, tx, runID, wave)
}

func nodeAttemptForNodeRunTx(ctx context.Context, tx *sql.Tx, nodeRunID string) (int, error) {
	var attempt int
	err := tx.QueryRowContext(ctx, `SELECT attempt FROM node_runs WHERE node_run_id = ?`, strings.TrimSpace(nodeRunID)).Scan(&attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, &EntityNotFoundError{EntityType: EntityTypeNode, NodeRunID: strings.TrimSpace(nodeRunID)}
	}
	if err != nil {
		return 0, fmt.Errorf("load node attempt for node_run=%q: %w", nodeRunID, err)
	}
	return attempt, nil
}

func nodeWaveForNodeRunTx(ctx context.Context, tx *sql.Tx, nodeRunID string) (int, error) {
	var wave int
	err := tx.QueryRowContext(ctx, `SELECT wave FROM node_runs WHERE node_run_id = ?`, strings.TrimSpace(nodeRunID)).Scan(&wave)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, &EntityNotFoundError{EntityType: EntityTypeNode, NodeRunID: strings.TrimSpace(nodeRunID)}
	}
	if err != nil {
		return 0, fmt.Errorf("load node wave for node_run=%q: %w", nodeRunID, err)
	}
	return wave, nil
}

func validateTransition(entityType EntityType, fromState, toState string) error {
	var transitions map[string]map[string]struct{}

	switch entityType {
	case EntityTypeRun:
		transitions = legalRunTransitions
	case EntityTypeNode:
		transitions = legalNodeTransitions
	case EntityTypePlan:
		transitions = legalPlanTransitions
	default:
		return &IllegalTransitionError{EntityType: entityType, FromState: fromState, ToState: toState}
	}

	next, ok := transitions[fromState]
	if !ok {
		return &IllegalTransitionError{EntityType: entityType, FromState: fromState, ToState: toState}
	}

	if _, ok := next[toState]; !ok {
		return &IllegalTransitionError{EntityType: entityType, FromState: fromState, ToState: toState}
	}

	return nil
}

func (s *TransitionService) applyEntityState(ctx context.Context, tx *sql.Tx, req TransitionRequest) error {
	var result sql.Result
	var err error

	switch req.Entity {
	case EntityTypeRun:
		pendingApprovalKey, keyErr := pendingApprovalGateKey("final", req)
		if req.ToState != "awaiting_human_confirmation" {
			pendingApprovalKey = ""
		}
		if keyErr != nil && req.ToState == "awaiting_human_confirmation" {
			return keyErr
		}
		result, err = tx.ExecContext(ctx,
			`UPDATE workflow_runs
			 SET run_status = ?,
			     pending_approval_key = CASE
			         WHEN ? = 'awaiting_human_confirmation' THEN ?
			         WHEN ? IN ('succeeded', 'failed', 'cancelled') THEN ''
			         WHEN ? = 'awaiting_human_confirmation' THEN ''
			         ELSE pending_approval_key
			     END,
			     row_version = row_version + 1,
			     updated_at = ?
			 WHERE run_id = ? AND run_status = ?`,
			req.ToState,
			req.ToState,
			pendingApprovalKey,
			req.ToState,
			req.FromState,
			req.OccurredAt,
			req.RunID,
			req.FromState,
		)
	case EntityTypePlan:
		reworkCycleDelta := 0
		if req.FromState == "awaiting_king_decision" && req.ToState == "needs_rework" {
			reworkCycleDelta = 1
		}
		pendingApprovalKey, keyErr := pendingApprovalGateKey("plan", req)
		if req.ToState != "awaiting_king_decision" {
			pendingApprovalKey = ""
		}
		if keyErr != nil && req.ToState == "awaiting_king_decision" {
			return keyErr
		}
		result, err = tx.ExecContext(ctx,
			`UPDATE workflow_runs
			 SET plan_state = ?,
			     pending_approval_key = CASE
			         WHEN ? = 'awaiting_king_decision' THEN ?
			         WHEN ? = 'awaiting_king_decision' THEN ''
			         ELSE pending_approval_key
			     END,
			     rework_cycle_count = rework_cycle_count + ?,
			     row_version = row_version + 1,
			     updated_at = ?
			 WHERE run_id = ? AND plan_state = ?`,
			req.ToState,
			req.ToState,
			pendingApprovalKey,
			req.FromState,
			reworkCycleDelta,
			req.OccurredAt,
			req.RunID,
			req.FromState,
		)
	case EntityTypeNode:
		result, err = tx.ExecContext(ctx,
			`UPDATE node_runs
			 SET node_status = ?,
			     row_version = row_version + 1,
			     started_at = CASE WHEN ? = 'running' AND started_at IS NULL THEN ? ELSE started_at END,
			     finished_at = CASE WHEN ? IN ('succeeded', 'failed', 'blocked', 'cancelled', 'skipped') THEN ? ELSE finished_at END
			 WHERE node_run_id = ? AND run_id = ? AND node_status = ?`,
			req.ToState,
			req.ToState,
			req.OccurredAt,
			req.ToState,
			req.OccurredAt,
			req.NodeRunID.String,
			req.RunID,
			req.FromState,
		)
	default:
		return &IllegalTransitionError{EntityType: req.Entity, FromState: req.FromState, ToState: req.ToState}
	}
	if err != nil {
		return fmt.Errorf("update %s state: %w", req.Entity, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for %s update: %w", req.Entity, err)
	}

	if rows == 1 {
		if req.Entity == EntityTypeNode {
			if err := s.touchRunActivityTimestamp(ctx, tx, req.RunID, req.OccurredAt); err != nil {
				return err
			}
		}
		return nil
	}

	actualState, found, err := s.loadCurrentState(ctx, tx, req)
	if err != nil {
		return err
	}
	if !found {
		return &EntityNotFoundError{EntityType: req.Entity, RunID: req.RunID, NodeRunID: req.NodeRunID.String}
	}

	return &StateConflictError{EntityType: req.Entity, ExpectedState: req.FromState, ActualState: actualState}
}

func (s *TransitionService) touchRunActivityTimestamp(ctx context.Context, tx *sql.Tx, runID, occurredAt string) error {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(occurredAt) == "" {
		return nil
	}

	_, err := tx.ExecContext(ctx,
		`UPDATE workflow_runs
		 SET updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
		 WHERE run_id = ?`,
		occurredAt,
		occurredAt,
		runID,
	)
	if err != nil {
		return fmt.Errorf("touch run activity timestamp: %w", err)
	}

	return nil
}

func markNodeAttemptSupersededTx(ctx context.Context, tx *sql.Tx, nodeRunID, supersededAt string) error {
	if strings.TrimSpace(nodeRunID) == "" {
		return fmt.Errorf("node_run_id is required")
	}
	if strings.TrimSpace(supersededAt) == "" {
		supersededAt = time.Now().UTC().Format(time.RFC3339)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE node_runs
		 SET superseded_at = ?
		 WHERE node_run_id = ?`,
		supersededAt,
		strings.TrimSpace(nodeRunID),
	); err != nil {
		return fmt.Errorf("mark node attempt superseded: %w", err)
	}
	return nil
}

func (s *TransitionService) loadCurrentState(ctx context.Context, tx *sql.Tx, req TransitionRequest) (string, bool, error) {
	var query string
	var arg any

	switch req.Entity {
	case EntityTypeRun:
		query = `SELECT run_status FROM workflow_runs WHERE run_id = ?`
		arg = req.RunID
	case EntityTypePlan:
		query = `SELECT plan_state FROM workflow_runs WHERE run_id = ?`
		arg = req.RunID
	case EntityTypeNode:
		query = `SELECT node_status FROM node_runs WHERE node_run_id = ? AND run_id = ?`
		var state string
		err := tx.QueryRowContext(ctx, query, req.NodeRunID.String, req.RunID).Scan(&state)
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		if err != nil {
			return "", false, fmt.Errorf("load current %s state: %w", req.Entity, err)
		}
		return state, true, nil
	default:
		return "", false, &IllegalTransitionError{EntityType: req.Entity, FromState: req.FromState, ToState: req.ToState}
	}

	var state string
	err := tx.QueryRowContext(ctx, query, arg).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("load current %s state: %w", req.Entity, err)
	}

	return state, true, nil
}

func (s *TransitionService) insertTransition(ctx context.Context, tx *sql.Tx, req TransitionRequest) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.TransitionID,
		req.RunID,
		req.NodeRunID,
		req.Entity,
		req.FromState,
		req.ToState,
		req.ActorID,
		req.RoleID,
		req.AuthoritySource,
		req.ReasonCode,
		req.ReasonText,
		req.CorrelationID,
		req.CausationID,
		req.TransitionPrevHash,
		req.TransitionEntryHash,
		req.OccurredAt,
	)
	return err
}

func (s *TransitionService) insertEvent(ctx context.Context, tx *sql.Tx, req TransitionRequest) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.EventID,
		req.RunID,
		req.RunEventSeq,
		req.NodeRunID,
		req.AttemptID,
		req.SchemaVersion,
		req.EventType,
		req.EventSeq,
		req.PayloadJSON,
		req.ActorID,
		req.RoleID,
		req.AuthoritySource,
		req.ReasonCode,
		req.ReasonText,
		req.CorrelationID,
		req.CausationID,
		req.EventPrevHash,
		req.EventEntryHash,
		req.OccurredAt,
	)
	if err != nil {
		return err
	}
	shouldProject := ShouldProjectRunCollectionEvent(req.EventType)
	if !req.SkipRunCollectionProjection && shouldProject {
		if err := AppendRunCollectionEventTx(ctx, tx, RunCollectionEventAppendRequest{
			EventID:     req.EventID,
			RunID:       req.RunID,
			EventType:   req.EventType,
			PayloadJSON: req.PayloadJSON,
			CreatedAt:   req.OccurredAt,
		}); err != nil {
			return err
		}
	}
	if err := persistAttemptExecutionProgressTx(ctx, tx, req.AttemptID, req.RunID, req.NodeRunID.String, req.EventType, req.ReasonCode, req.ToState, req.OccurredAt); err != nil {
		return err
	}
	mailboxProjectionAllowed, err := nodeRunMailboxProjectionAllowedTx(ctx, tx, req.NodeRunID.String)
	if err != nil {
		return err
	}
	if mailboxProjectionAllowed {
		_ = MirrorEventToMailbox(ctx, tx, s.roleCommunicationPolicy, EventMailboxMirrorRequest{
			EventID:     req.EventID,
			RunID:       req.RunID,
			NodeRunID:   req.NodeRunID,
			FromState:   req.FromState,
			ToState:     req.ToState,
			AttemptID:   req.AttemptID,
			EventType:   req.EventType,
			PayloadJSON: req.PayloadJSON,
			RoleID:      req.RoleID,
			ReasonCode:  req.ReasonCode,
			ReasonText:  req.ReasonText,
			CreatedAt:   req.OccurredAt,
		})
	}
	return err
}
