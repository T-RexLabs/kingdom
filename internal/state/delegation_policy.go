package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type DelegationOutcome string

const (
	DelegationOutcomeSuccess  DelegationOutcome = "success"
	DelegationOutcomeHardFail DelegationOutcome = "hard_fail"
)

const (
	delegationReasonPolicyViolation    = "policy_violation"
	delegationReasonDependencyBlocked  = "dependency_blocked"
	delegationReasonSpawnDepthExceeded = "spawn_depth_exceeded"
	delegationReasonLineageCycle       = "lineage_cycle"
)

var protectedGovernanceNodeClasses = map[string]struct{}{
	"execution_kickoff":   {},
	"delegated_execution": {},
	"execution_review":    {},
	"validation_gates":    {},
	"royal_closure":       {},
}

var governancePrerequisiteNodeClasses = []string{
	"decree_intake",
	"advisor_consultation",
	"advisory_synthesis",
	"plan_drafting",
	"king_plan_decision",
}

var lockedSpawnMatrix = map[string]map[string]struct{}{
	"king": {
		"chancellor":       {},
		"council-member":   {},
		"noble-delegate":   {},
		"clergy-delegate":  {},
		"burgher-delegate": {},
		"peasant-delegate": {},
	},
	"chancellor": {
		"council-member": {},
	},
	"council-member": {
		"builder-berserker":       {},
		"reviewer-shieldmaiden":   {},
		"intelligence-qa-scout":   {},
		"intelligence-nightraven": {},
		"intelligence-loki-chaos": {},
	},
}

type DelegationRequest struct {
	RunID            string
	NodeID           string
	DelegatorAgentID string
	DelegateAgentID  string
	DelegateRoleID   string
	MaxSpawnDepth    int
	OccurredAt       string
}

type DelegationDecision struct {
	Outcome          DelegationOutcome
	ReasonCode       string
	EscalationTarget string
}

type DelegationPolicyEngine struct {
	db       *sql.DB
	registry *AgentRegistry
	mailbox  *MailboxStore
}

func NewDelegationPolicyEngine(db *sql.DB, registry *AgentRegistry) *DelegationPolicyEngine {
	return &DelegationPolicyEngine{db: db, registry: registry}
}

func NewDelegationPolicyEngineWithMailbox(db *sql.DB, registry *AgentRegistry, mailbox *MailboxStore) *DelegationPolicyEngine {
	return &DelegationPolicyEngine{db: db, registry: registry, mailbox: mailbox}
}

func (e *DelegationPolicyEngine) Delegate(ctx context.Context, req DelegationRequest) (DelegationDecision, error) {
	if req.MaxSpawnDepth < 0 {
		return DelegationDecision{}, fmt.Errorf("max spawn depth must be non-negative")
	}
	if req.MaxSpawnDepth == 0 {
		req.MaxSpawnDepth = DefaultMaxSpawnDepth
	}

	decision, delegateRole, delegateExists, err := e.evaluate(ctx, req)
	if err != nil {
		return DelegationDecision{}, err
	}
	if decision.Outcome != DelegationOutcomeSuccess {
		if err := e.writeDelegationMailboxEscalation(ctx, req, decision); err != nil {
			return DelegationDecision{}, err
		}
		return decision, nil
	}

	if delegateExists {
		err = e.registry.ReassignAgent(ctx, ReassignAgentRequest{
			AgentID:          req.DelegateAgentID,
			NewParentAgentID: req.DelegatorAgentID,
			MaxSpawnDepth:    req.MaxSpawnDepth,
			UpdatedAt:        req.OccurredAt,
		})
	} else {
		err = e.registry.SpawnAgent(ctx, SpawnAgentRequest{
			AgentID:       req.DelegateAgentID,
			RoleID:        delegateRole,
			Status:        AgentStatusIdle,
			ParentAgentID: req.DelegatorAgentID,
			MaxSpawnDepth: req.MaxSpawnDepth,
			CreatedAt:     req.OccurredAt,
			UpdatedAt:     req.OccurredAt,
		})
	}
	if err != nil {
		var depthErr *SpawnDepthExceededError
		if errors.As(err, &depthErr) {
			return DelegationDecision{Outcome: DelegationOutcomeHardFail, ReasonCode: delegationReasonSpawnDepthExceeded, EscalationTarget: "chancellor"}, nil
		}

		var cycleErr *LineageCycleError
		if errors.As(err, &cycleErr) {
			return DelegationDecision{Outcome: DelegationOutcomeHardFail, ReasonCode: delegationReasonLineageCycle, EscalationTarget: "chancellor"}, nil
		}

		return DelegationDecision{}, err
	}

	return decision, nil
}

func (e *DelegationPolicyEngine) evaluate(ctx context.Context, req DelegationRequest) (DelegationDecision, string, bool, error) {
	if req.RunID == "" || req.NodeID == "" || req.DelegatorAgentID == "" || req.DelegateAgentID == "" {
		return DelegationDecision{}, "", false, fmt.Errorf("run id, node id, delegator agent id, and delegate agent id are required")
	}

	delegatorRole, canSpawn, err := e.loadRoleCapabilities(ctx, req.DelegatorAgentID)
	if err != nil {
		return DelegationDecision{}, "", false, err
	}
	if !canSpawn {
		return DelegationDecision{Outcome: DelegationOutcomeHardFail, ReasonCode: delegationReasonPolicyViolation, EscalationTarget: "chancellor"}, "", false, nil
	}

	delegateRole, delegateExists, err := e.resolveDelegateRole(ctx, req.DelegateAgentID, req.DelegateRoleID)
	if err != nil {
		return DelegationDecision{}, "", false, err
	}

	if !isSpawnableTarget(delegatorRole, delegateRole) {
		return DelegationDecision{Outcome: DelegationOutcomeHardFail, ReasonCode: delegationReasonPolicyViolation, EscalationTarget: "chancellor"}, delegateRole, delegateExists, nil
	}

	requiredRole, err := e.requiredRoleForNode(ctx, req.RunID, req.NodeID)
	if err != nil {
		return DelegationDecision{}, "", false, err
	}
	if requiredRole != delegateRole {
		return DelegationDecision{Outcome: DelegationOutcomeHardFail, ReasonCode: delegationReasonPolicyViolation, EscalationTarget: "chancellor"}, delegateRole, delegateExists, nil
	}

	blocked, err := e.isGovernanceBlocked(ctx, req.RunID, req.NodeID)
	if err != nil {
		return DelegationDecision{}, "", false, err
	}
	if blocked {
		return DelegationDecision{Outcome: DelegationOutcomeHardFail, ReasonCode: delegationReasonDependencyBlocked, EscalationTarget: "chancellor"}, delegateRole, delegateExists, nil
	}

	if req.DelegateAgentID == req.DelegatorAgentID {
		return DelegationDecision{Outcome: DelegationOutcomeHardFail, ReasonCode: delegationReasonLineageCycle, EscalationTarget: "chancellor"}, delegateRole, delegateExists, nil
	}

	if delegateExists {
		hasCycle, err := e.wouldCreateCycle(ctx, req.DelegateAgentID, req.DelegatorAgentID)
		if err != nil {
			return DelegationDecision{}, "", false, err
		}
		if hasCycle {
			return DelegationDecision{Outcome: DelegationOutcomeHardFail, ReasonCode: delegationReasonLineageCycle, EscalationTarget: "chancellor"}, delegateRole, delegateExists, nil
		}
	}

	return DelegationDecision{Outcome: DelegationOutcomeSuccess}, delegateRole, delegateExists, nil
}

func (e *DelegationPolicyEngine) writeDelegationMailboxEscalation(ctx context.Context, req DelegationRequest, decision DelegationDecision) error {
	if e.mailbox == nil {
		return nil
	}
	if decision.Outcome != DelegationOutcomeHardFail || decision.EscalationTarget == "" {
		return nil
	}

	toAgentID, err := e.resolveEscalationAgentID(ctx, req.RunID, decision.EscalationTarget)
	if err != nil {
		return err
	}

	payload := map[string]any{
		"run_id":             req.RunID,
		"node_id":            req.NodeID,
		"delegator_agent_id": req.DelegatorAgentID,
		"delegate_agent_id":  req.DelegateAgentID,
		"delegate_role_id":   req.DelegateRoleID,
		"outcome":            decision.Outcome,
		"reason_code":        decision.ReasonCode,
		"authority_source":   "delegation_policy",
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal delegation escalation payload: %w", err)
	}

	_, err = e.mailbox.SendMessage(ctx, SendMailboxMessageRequest{
		RunID:       req.RunID,
		FromAgentID: req.DelegatorAgentID,
		ToAgentID:   toAgentID,
		ThreadID:    fmt.Sprintf("thread_%s_delegation_%s", req.RunID, req.NodeID),
		MessageType: MailboxMessageTypeEscalation,
		Priority:    MailboxPriorityHigh,
		PayloadJSON: string(payloadBytes),
		CreatedAt:   req.OccurredAt,
	})
	if err != nil {
		return fmt.Errorf("write delegation mailbox escalation: %w", err)
	}

	return nil
}

func (e *DelegationPolicyEngine) resolveEscalationAgentID(ctx context.Context, runID, roleID string) (string, error) {
	var agentID string
	err := e.db.QueryRowContext(ctx,
		`SELECT DISTINCT a.agent_id
		 FROM node_runs nr
		 JOIN agents a ON a.agent_id = nr.agent_id
		 WHERE nr.run_id = ?
		   AND a.role_id = ?
		 ORDER BY a.agent_id ASC
		 LIMIT 1`,
		runID,
		roleID,
	).Scan(&agentID)
	if err == nil {
		return agentID, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("resolve escalation agent in run for role=%q: %w", roleID, err)
	}

	err = e.db.QueryRowContext(ctx,
		`SELECT agent_id
		 FROM agents
		 WHERE role_id = ?
		 ORDER BY agent_id ASC
		 LIMIT 1`,
		roleID,
	).Scan(&agentID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("escalation agent not found for role=%q", roleID)
	}
	if err != nil {
		return "", fmt.Errorf("resolve escalation agent for role=%q: %w", roleID, err)
	}

	return agentID, nil
}

func (e *DelegationPolicyEngine) loadRoleCapabilities(ctx context.Context, agentID string) (string, bool, error) {
	var roleID string
	var canSpawn bool
	err := e.db.QueryRowContext(ctx,
		`SELECT a.role_id, r.can_spawn = 1
		 FROM agents a
		 JOIN roles r ON r.role_id = a.role_id
		 WHERE a.agent_id = ?`,
		agentID,
	).Scan(&roleID, &canSpawn)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, &AgentNotFoundError{AgentID: agentID}
	}
	if err != nil {
		return "", false, fmt.Errorf("load role capabilities for %q: %w", agentID, err)
	}
	return roleID, canSpawn, nil
}

func (e *DelegationPolicyEngine) resolveDelegateRole(ctx context.Context, delegateAgentID, requestedRole string) (string, bool, error) {
	var existingRole string
	err := e.db.QueryRowContext(ctx, `SELECT role_id FROM agents WHERE agent_id = ?`, delegateAgentID).Scan(&existingRole)
	if err == nil {
		if requestedRole != "" && requestedRole != existingRole {
			return "", true, fmt.Errorf("delegate role mismatch for %q: requested %q existing %q", delegateAgentID, requestedRole, existingRole)
		}
		return existingRole, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", false, fmt.Errorf("load delegate role for %q: %w", delegateAgentID, err)
	}
	if requestedRole == "" {
		return "", false, fmt.Errorf("delegate role is required when spawning new agent")
	}

	var found int
	err = e.db.QueryRowContext(ctx, `SELECT 1 FROM roles WHERE role_id = ?`, requestedRole).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, fmt.Errorf("delegate role %q not found", requestedRole)
	}
	if err != nil {
		return "", false, fmt.Errorf("verify delegate role %q: %w", requestedRole, err)
	}

	return requestedRole, false, nil
}

func isSpawnableTarget(delegatorRole, delegateRole string) bool {
	targets, ok := lockedSpawnMatrix[delegatorRole]
	if !ok {
		return false
	}
	_, ok = targets[delegateRole]
	return ok
}

func (e *DelegationPolicyEngine) requiredRoleForNode(ctx context.Context, runID, nodeID string) (string, error) {
	var requiredRole string
	err := e.db.QueryRowContext(ctx,
		`SELECT wn.required_role
		 FROM workflow_runs wr
		 JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id
		 WHERE wr.run_id = ? AND wn.node_id = ?`,
		runID,
		nodeID,
	).Scan(&requiredRole)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("required role not found for run=%q node=%q", runID, nodeID)
	}
	if err != nil {
		return "", fmt.Errorf("load required role for run=%q node=%q: %w", runID, nodeID, err)
	}
	return requiredRole, nil
}

func (e *DelegationPolicyEngine) isGovernanceBlocked(ctx context.Context, runID, nodeID string) (bool, error) {
	nodeClass := nodeClassFromID(nodeID)
	if _, restricted := protectedGovernanceNodeClasses[nodeClass]; !restricted {
		return false, nil
	}

	var planState string
	err := e.db.QueryRowContext(ctx, `SELECT plan_state FROM workflow_runs WHERE run_id = ?`, runID).Scan(&planState)
	if errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("run not found: %q", runID)
	}
	if err != nil {
		return false, fmt.Errorf("load plan state for run=%q: %w", runID, err)
	}

	if planState != "approved" {
		return true, nil
	}

	for _, prerequisiteNodeClass := range governancePrerequisiteNodeClasses {
		var totalNodes int
		err := e.db.QueryRowContext(ctx,
			`SELECT COUNT(1)
			 FROM workflow_nodes wn
			 JOIN workflow_runs wr ON wr.workflow_definition_id = wn.workflow_definition_id
			 WHERE wr.run_id = ? AND wn.node_class = ?`,
			runID,
			prerequisiteNodeClass,
		).Scan(&totalNodes)
		if err != nil {
			return false, fmt.Errorf("count prerequisite node class for run=%q class=%q: %w", runID, prerequisiteNodeClass, err)
		}
		if totalNodes == 0 {
			continue
		}

		var succeededCount int
		err = e.db.QueryRowContext(ctx,
			`SELECT COUNT(1)
			 FROM node_runs nr
			 JOIN workflow_runs wr ON wr.run_id = nr.run_id
			 JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
			 WHERE nr.run_id = ? AND wn.node_class = ? AND nr.node_status = 'succeeded'`,
			runID,
			prerequisiteNodeClass,
		).Scan(&succeededCount)
		if err != nil {
			return false, fmt.Errorf("check prerequisite class status for run=%q class=%q: %w", runID, prerequisiteNodeClass, err)
		}
		if succeededCount < totalNodes {
			return true, nil
		}
	}

	return false, nil
}

func nodeClassFromID(nodeID string) string {
	value := strings.TrimSpace(strings.ToLower(nodeID))
	for _, classID := range []string{
		"decree_intake",
		"advisor_consultation",
		"advisory_synthesis",
		"plan_drafting",
		"king_plan_decision",
		"execution_kickoff",
		"delegated_execution",
		"execution_review",
		"validation_gates",
		"royal_closure",
	} {
		if value == classID || strings.HasPrefix(value, classID+"_") {
			return classID
		}
	}
	return value
}

func (e *DelegationPolicyEngine) wouldCreateCycle(ctx context.Context, agentID, newParentAgentID string) (bool, error) {
	var found string
	err := e.db.QueryRowContext(ctx,
		`WITH RECURSIVE lineage(agent_id, parent_agent_id) AS (
			SELECT agent_id, parent_agent_id FROM agents WHERE agent_id = ?
			UNION ALL
			SELECT a.agent_id, a.parent_agent_id
			FROM agents a
			JOIN lineage l ON a.agent_id = l.parent_agent_id
			WHERE l.parent_agent_id IS NOT NULL
		)
		SELECT agent_id FROM lineage WHERE agent_id = ? LIMIT 1`,
		newParentAgentID,
		agentID,
	).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check lineage cycle for %q -> %q: %w", agentID, newParentAgentID, err)
	}
	return true, nil
}
