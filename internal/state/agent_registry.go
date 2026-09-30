package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/sqlitex"
)

type agentRegistryReadQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type agentRegistryRowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type agentRegistryExecQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type AgentStatus string

const (
	AgentStatusIdle    AgentStatus = "idle"
	AgentStatusBusy    AgentStatus = "busy"
	AgentStatusBlocked AgentStatus = "blocked"
	AgentStatusOffline AgentStatus = "offline"
	AgentStatusFailed  AgentStatus = "failed"
)

type AgentNotFoundError struct {
	AgentID string
}

func (e *AgentNotFoundError) Error() string {
	return fmt.Sprintf("agent not found: %q", e.AgentID)
}

type IllegalAgentStatusTransitionError struct {
	FromStatus AgentStatus
	ToStatus   AgentStatus
}

func (e *IllegalAgentStatusTransitionError) Error() string {
	return fmt.Sprintf("illegal agent status transition: %q -> %q", e.FromStatus, e.ToStatus)
}

type AgentStatusConflictError struct {
	AgentID        string
	ExpectedStatus AgentStatus
	ObservedStatus AgentStatus
}

func (e *AgentStatusConflictError) Error() string {
	return fmt.Sprintf("agent status conflict for %q: expected %q got %q", e.AgentID, e.ExpectedStatus, e.ObservedStatus)
}

type SpawnDepthExceededError struct {
	AgentID       string
	ComputedDepth int
	MaxDepth      int
}

func (e *SpawnDepthExceededError) Error() string {
	return fmt.Sprintf("spawn depth exceeded for %q: computed=%d max=%d", e.AgentID, e.ComputedDepth, e.MaxDepth)
}

type LineageCycleError struct {
	AgentID       string
	ParentAgentID string
}

func (e *LineageCycleError) Error() string {
	return fmt.Sprintf("lineage cycle detected for agent %q with parent %q", e.AgentID, e.ParentAgentID)
}

type MissingResponsibleAgentError struct {
	RunID   string
	NodeID  string
	Attempt int
}

func (e *MissingResponsibleAgentError) Error() string {
	return fmt.Sprintf("missing responsible agent for run=%q node=%q attempt=%d", e.RunID, e.NodeID, e.Attempt)
}

type SpawnAgentRequest struct {
	AgentID       string
	RoleID        string
	Status        AgentStatus
	ParentAgentID string
	MaxSpawnDepth int
	CreatedAt     string
	UpdatedAt     string
}

type UpdateAgentStatusRequest struct {
	AgentID    string
	FromStatus AgentStatus
	ToStatus   AgentStatus
	UpdatedAt  string
}

type ReassignAgentRequest struct {
	AgentID          string
	NewParentAgentID string
	MaxSpawnDepth    int
	UpdatedAt        string
}

type CreateNodeAttemptRequest struct {
	NodeRunID              string
	RunID                  string
	NodeID                 string
	Attempt                int
	Wave                   int
	NodeStatus             string
	RequiredRole           string
	AgentID                string
	RowVersion             int
	HandoffSourceAttemptID string
	CreatedAt              string
	StartedAt              sql.NullString
	FinishedAt             sql.NullString
}

type RunParticipant struct {
	AgentID       string
	RoleID        string
	DisplayName   string
	Status        AgentStatus
	ParentAgentID sql.NullString
	SpawnDepth    int
	FirstSeenAt   string
}

type AgentRecord struct {
	AgentID       string
	RoleID        string
	DisplayName   string
	Status        AgentStatus
	ParentAgentID sql.NullString
	SpawnDepth    int
	CreatedAt     string
}

type AgentRegistry struct {
	db *sql.DB
}

func NewAgentRegistry(db *sql.DB) *AgentRegistry {
	return &AgentRegistry{db: db}
}

var legalAgentStatusTransitions = map[AgentStatus]map[AgentStatus]struct{}{
	AgentStatusIdle: {
		AgentStatusBusy:    {},
		AgentStatusBlocked: {},
		AgentStatusOffline: {},
		AgentStatusFailed:  {},
	},
	AgentStatusBusy: {
		AgentStatusIdle:    {},
		AgentStatusBlocked: {},
		AgentStatusOffline: {},
		AgentStatusFailed:  {},
	},
	AgentStatusBlocked: {
		AgentStatusIdle:    {},
		AgentStatusBusy:    {},
		AgentStatusOffline: {},
		AgentStatusFailed:  {},
	},
	AgentStatusOffline: {
		AgentStatusIdle:   {},
		AgentStatusFailed: {},
	},
	AgentStatusFailed: {},
}

func (r *AgentRegistry) SpawnAgent(ctx context.Context, req SpawnAgentRequest) error {
	if req.MaxSpawnDepth < 0 {
		return fmt.Errorf("max spawn depth must be non-negative")
	}

	parentDepth, err := r.agentDepth(ctx, req.ParentAgentID)
	if err != nil {
		return err
	}

	spawnDepth := parentDepth + 1
	if spawnDepth > req.MaxSpawnDepth {
		return &SpawnDepthExceededError{AgentID: req.AgentID, ComputedDepth: spawnDepth, MaxDepth: req.MaxSpawnDepth}
	}

	_, err = r.db.ExecContext(ctx,
		`INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		req.AgentID,
		req.RoleID,
		req.Status,
		req.ParentAgentID,
		spawnDepth,
		req.CreatedAt,
		req.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert spawned agent: %w", err)
	}

	return nil
}

func (r *AgentRegistry) UpdateAgentStatus(ctx context.Context, req UpdateAgentStatusRequest) error {
	if err := validateAgentStatusTransition(req.FromStatus, req.ToStatus); err != nil {
		return err
	}

	result, err := r.db.ExecContext(ctx,
		`UPDATE agents SET status = ?, updated_at = ? WHERE agent_id = ? AND status = ?`,
		req.ToStatus,
		req.UpdatedAt,
		req.AgentID,
		req.FromStatus,
	)
	if err != nil {
		return fmt.Errorf("update agent status: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("agent status rows affected: %w", err)
	}
	if rows == 1 {
		return nil
	}

	observedStatus, found, err := r.agentStatus(ctx, req.AgentID)
	if err != nil {
		return err
	}
	if !found {
		return &AgentNotFoundError{AgentID: req.AgentID}
	}

	return &AgentStatusConflictError{AgentID: req.AgentID, ExpectedStatus: req.FromStatus, ObservedStatus: observedStatus}
}

func projectedAgentStatusFromLiveNodeRunsWithQuerier(ctx context.Context, q agentRegistryRowQuerier, agentID string) (AgentStatus, error) {
	return projectedAgentStatusFromLiveNodeRunsWithQuerierExcludingNode(ctx, q, agentID, "")
}

func projectedAgentStatusFromLiveNodeRunsWithQuerierExcludingNode(ctx context.Context, q agentRegistryRowQuerier, agentID string, excludeNodeRunID string) (AgentStatus, error) {
	trimmedAgentID := strings.TrimSpace(agentID)
	if trimmedAgentID == "" {
		return AgentStatusIdle, nil
	}
	trimmedExcludeNodeRunID := strings.TrimSpace(excludeNodeRunID)
	var status AgentStatus
	err := q.QueryRowContext(ctx, `
SELECT CASE
	WHEN EXISTS (
		SELECT 1
		FROM node_runs
		WHERE agent_id = ?
		  AND (? = '' OR node_run_id <> ?)
		  AND COALESCE(TRIM(superseded_at), '') = ''
		  AND node_status IN ('starting', 'running')
	) THEN 'busy'
	WHEN EXISTS (
		SELECT 1
		FROM node_runs
		WHERE agent_id = ?
		  AND (? = '' OR node_run_id <> ?)
		  AND COALESCE(TRIM(superseded_at), '') = ''
		  AND node_status = 'blocked'
	) THEN 'blocked'
	ELSE 'idle'
END`, trimmedAgentID, trimmedExcludeNodeRunID, trimmedExcludeNodeRunID, trimmedAgentID, trimmedExcludeNodeRunID, trimmedExcludeNodeRunID).Scan(&status)
	if err != nil {
		return "", fmt.Errorf("project live agent status for %q: %w", trimmedAgentID, err)
	}
	return status, nil
}

func syncAgentOperationalStatusWithQuerier(ctx context.Context, q agentRegistryExecQuerier, agentID, updatedAt string) error {
	trimmedAgentID := strings.TrimSpace(agentID)
	if trimmedAgentID == "" {
		return nil
	}
	desiredStatus, err := projectedAgentStatusFromLiveNodeRunsWithQuerier(ctx, q, trimmedAgentID)
	if err != nil {
		return err
	}
	if _, err := q.ExecContext(ctx,
		`UPDATE agents
		 SET status = ?,
		     updated_at = ?
		 WHERE agent_id = ?
		   AND status NOT IN ('offline', 'failed')`,
		desiredStatus,
		updatedAt,
		trimmedAgentID,
	); err != nil {
		return fmt.Errorf("sync agent operational status for %q: %w", trimmedAgentID, err)
	}
	return nil
}

func (r *AgentRegistry) SyncOperationalStatus(ctx context.Context, agentID, updatedAt string) error {
	if r == nil || r.db == nil {
		return nil
	}
	return syncAgentOperationalStatusWithQuerier(ctx, r.db, agentID, updatedAt)
}

func (r *AgentRegistry) ReassignAgent(ctx context.Context, req ReassignAgentRequest) error {
	_, err := sqlitex.Retry(ctx, func() error {
		return r.reassignAgentOnce(ctx, req)
	}, sqlitex.Options{})
	return err
}

func (r *AgentRegistry) reassignAgentOnce(ctx context.Context, req ReassignAgentRequest) error {
	if req.MaxSpawnDepth < 0 {
		return fmt.Errorf("max spawn depth must be non-negative")
	}
	if req.AgentID == req.NewParentAgentID {
		return &LineageCycleError{AgentID: req.AgentID, ParentAgentID: req.NewParentAgentID}
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin reassign transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if _, err := r.agentDepthTx(ctx, tx, req.AgentID); err != nil {
		return err
	}

	newParentDepth, err := r.agentDepthTx(ctx, tx, req.NewParentAgentID)
	if err != nil {
		return err
	}

	hasCycle, err := r.wouldCreateCycle(ctx, tx, req.AgentID, req.NewParentAgentID)
	if err != nil {
		return err
	}
	if hasCycle {
		return &LineageCycleError{AgentID: req.AgentID, ParentAgentID: req.NewParentAgentID}
	}

	maxDistance, err := r.maxSubtreeDistance(ctx, tx, req.AgentID)
	if err != nil {
		return err
	}

	newRootDepth := newParentDepth + 1
	if newRootDepth+maxDistance > req.MaxSpawnDepth {
		return &SpawnDepthExceededError{AgentID: req.AgentID, ComputedDepth: newRootDepth + maxDistance, MaxDepth: req.MaxSpawnDepth}
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE agents SET parent_agent_id = ?, spawn_depth = ?, updated_at = ? WHERE agent_id = ?`,
		req.NewParentAgentID,
		newRootDepth,
		req.UpdatedAt,
		req.AgentID,
	); err != nil {
		return fmt.Errorf("update agent parent assignment: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`WITH RECURSIVE descendants(agent_id, depth_delta) AS (
			SELECT agent_id, 0 FROM agents WHERE agent_id = ?
			UNION ALL
			SELECT a.agent_id, d.depth_delta + 1
			FROM agents a
			JOIN descendants d ON a.parent_agent_id = d.agent_id
		)
		UPDATE agents
		SET spawn_depth = ? + (
			SELECT depth_delta FROM descendants WHERE descendants.agent_id = agents.agent_id
		),
		    updated_at = ?
		WHERE agent_id IN (
			SELECT agent_id FROM descendants WHERE depth_delta > 0
		)`,
		req.AgentID,
		newRootDepth,
		req.UpdatedAt,
	); err != nil {
		return fmt.Errorf("recompute descendant spawn depths: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit reassign transaction: %w", err)
	}
	committed = true

	return nil
}

func (r *AgentRegistry) CreateNodeAttempt(ctx context.Context, req CreateNodeAttemptRequest) error {
	if req.AgentID == "" {
		return &MissingResponsibleAgentError{RunID: req.RunID, NodeID: req.NodeID, Attempt: req.Attempt}
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin node attempt transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := insertNodeAttemptTx(ctx, tx, req); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit node attempt transaction: %w", err)
	}
	committed = true

	return nil
}

func (r *AgentRegistry) ListRunAgents(ctx context.Context, runID string) ([]RunParticipant, error) {
	rows, err := r.db.QueryContext(ctx,
		`WITH latest_run_agent_nodes AS (
			SELECT nr.agent_id,
			       nr.node_id,
			       nr.node_status,
			       ROW_NUMBER() OVER (
			         PARTITION BY nr.agent_id, nr.node_id
			         ORDER BY CASE
			           WHEN COALESCE(TRIM(nr.superseded_at), '') = '' THEN 0
			           ELSE 1
			         END ASC,
			         nr.attempt DESC,
			         nr.node_run_id DESC
			       ) AS node_rank
			FROM node_runs nr
			WHERE nr.run_id = ?
		), run_participant_status AS (
			SELECT agent_id,
			       CASE
			         WHEN SUM(CASE WHEN node_rank = 1 AND node_status IN ('starting', 'running') THEN 1 ELSE 0 END) > 0 THEN 'busy'
			         WHEN SUM(CASE WHEN node_rank = 1 AND node_status = 'blocked' THEN 1 ELSE 0 END) > 0 THEN 'blocked'
			         WHEN SUM(CASE WHEN node_rank = 1 AND node_status = 'failed' THEN 1 ELSE 0 END) > 0 THEN 'failed'
			         ELSE NULL
			       END AS status
			FROM latest_run_agent_nodes
			WHERE node_rank = 1
			GROUP BY agent_id
		)
		SELECT
			rp.agent_id,
			rp.role_id,
			rp.display_name,
			CASE
			  WHEN rps.status IS NOT NULL THEN rps.status
			  WHEN a.status IN ('offline', 'failed') THEN a.status
			  ELSE 'idle'
			END AS status,
			rp.parent_agent_id,
			rp.spawn_depth,
			rp.first_seen_at
		FROM run_participants rp
		JOIN agents a ON a.agent_id = rp.agent_id
		LEFT JOIN run_participant_status rps ON rps.agent_id = rp.agent_id
		WHERE rp.run_id = ?
		  AND rp.role_id != 'allfather'
		ORDER BY rp.spawn_depth ASC, rp.first_seen_at ASC, rp.agent_id ASC`,
		runID,
		runID,
	)
	if err != nil {
		return nil, fmt.Errorf("list run agents for %q: %w", runID, err)
	}
	defer rows.Close()

	agents := make([]RunParticipant, 0)
	for rows.Next() {
		var item RunParticipant
		if err := rows.Scan(
			&item.AgentID,
			&item.RoleID,
			&item.DisplayName,
			&item.Status,
			&item.ParentAgentID,
			&item.SpawnDepth,
			&item.FirstSeenAt,
		); err != nil {
			return nil, fmt.Errorf("scan run agent row for %q: %w", runID, err)
		}
		agents = append(agents, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate run agents for %q: %w", runID, err)
	}

	return agents, nil
}

func (r *AgentRegistry) CountRunAgents(ctx context.Context, runID string) (int, error) {
	return CountRunAgentsWithQuerier(ctx, r.db, runID)

}

func CountRunAgentsWithQuerier(ctx context.Context, q agentRegistryReadQuerier, runID string) (int, error) {
	var count int
	err := q.QueryRowContext(ctx,
		`SELECT COUNT(1)
		 FROM run_participants
		 WHERE run_id = ?
		   AND role_id != 'allfather'`,
		runID,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count run agents for %q: %w", runID, err)
	}
	return count, nil
}

func firstSeenAtFromNodeAttempt(req CreateNodeAttemptRequest) string {
	if trimmed := req.CreatedAt; trimmed != "" {
		return trimmed
	}
	if req.StartedAt.Valid {
		return req.StartedAt.String
	}
	if req.FinishedAt.Valid {
		return req.FinishedAt.String
	}
	return time.Now().UTC().Format(time.RFC3339)
}

// ListAllAgents returns every agent in the registry, regardless of run
// association. This is used by the agents page to always display the full
// court roster even when no run is active.
func (r *AgentRegistry) ListAllAgents(ctx context.Context) ([]AgentRecord, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT
			a.agent_id,
			a.role_id,
			COALESCE(NULLIF(TRIM(a.display_name), ''), role.display_name) AS display_name,
			a.status,
			a.parent_agent_id,
			a.spawn_depth,
			a.created_at
		FROM agents a
		JOIN roles role ON role.role_id = a.role_id
		WHERE a.role_id != 'allfather'
		ORDER BY a.spawn_depth ASC, a.created_at ASC, a.agent_id ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("list all agents: %w", err)
	}
	defer rows.Close()

	agents := make([]AgentRecord, 0)
	for rows.Next() {
		var item AgentRecord
		if err := rows.Scan(
			&item.AgentID,
			&item.RoleID,
			&item.DisplayName,
			&item.Status,
			&item.ParentAgentID,
			&item.SpawnDepth,
			&item.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan agent row: %w", err)
		}
		agents = append(agents, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate all agents: %w", err)
	}

	return agents, nil
}

func validateAgentStatusTransition(fromStatus, toStatus AgentStatus) error {
	next, ok := legalAgentStatusTransitions[fromStatus]
	if !ok {
		return &IllegalAgentStatusTransitionError{FromStatus: fromStatus, ToStatus: toStatus}
	}
	if _, ok := next[toStatus]; !ok {
		return &IllegalAgentStatusTransitionError{FromStatus: fromStatus, ToStatus: toStatus}
	}
	return nil
}

func (r *AgentRegistry) agentDepth(ctx context.Context, agentID string) (int, error) {
	return r.agentDepthQuery(ctx, r.db, agentID)
}

func (r *AgentRegistry) agentDepthTx(ctx context.Context, tx *sql.Tx, agentID string) (int, error) {
	return r.agentDepthQuery(ctx, tx, agentID)
}

type depthQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (r *AgentRegistry) agentDepthQuery(ctx context.Context, q depthQuerier, agentID string) (int, error) {
	var depth int
	err := q.QueryRowContext(ctx, `SELECT spawn_depth FROM agents WHERE agent_id = ?`, agentID).Scan(&depth)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, &AgentNotFoundError{AgentID: agentID}
	}
	if err != nil {
		return 0, fmt.Errorf("load agent depth for %q: %w", agentID, err)
	}
	return depth, nil
}

func (r *AgentRegistry) agentStatus(ctx context.Context, agentID string) (AgentStatus, bool, error) {
	var status AgentStatus
	err := r.db.QueryRowContext(ctx, `SELECT status FROM agents WHERE agent_id = ?`, agentID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("load agent status for %q: %w", agentID, err)
	}
	return status, true, nil
}

func (r *AgentRegistry) wouldCreateCycle(ctx context.Context, tx *sql.Tx, agentID, newParentAgentID string) (bool, error) {
	var found string
	err := tx.QueryRowContext(ctx,
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

func (r *AgentRegistry) maxSubtreeDistance(ctx context.Context, tx *sql.Tx, agentID string) (int, error) {
	var maxDistance int
	err := tx.QueryRowContext(ctx,
		`WITH RECURSIVE descendants(agent_id, depth_delta) AS (
			SELECT agent_id, 0 FROM agents WHERE agent_id = ?
			UNION ALL
			SELECT a.agent_id, d.depth_delta + 1
			FROM agents a
			JOIN descendants d ON a.parent_agent_id = d.agent_id
		)
		SELECT COALESCE(MAX(depth_delta), 0) FROM descendants`,
		agentID,
	).Scan(&maxDistance)
	if err != nil {
		return 0, fmt.Errorf("compute subtree distance for %q: %w", agentID, err)
	}
	return maxDistance, nil
}
