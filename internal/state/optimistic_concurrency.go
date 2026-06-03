package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type RowVersionConflictError struct {
	EntityType      string
	EntityID        string
	ExpectedVersion int64
	ActualVersion   int64
}

func (e *RowVersionConflictError) Error() string {
	return fmt.Sprintf(
		"row version conflict for %s %q: expected=%d actual=%d",
		e.EntityType,
		e.EntityID,
		e.ExpectedVersion,
		e.ActualVersion,
	)
}

type WorkflowRunUpdateCAS struct {
	RunID              string
	ExpectedRowVersion int64
	RunStatus          string
	PlanState          string
	UpdatedAt          string
}

type NodeRunUpdateCAS struct {
	NodeRunID          string
	RunID              string
	ExpectedRowVersion int64
	NodeStatus         string
}

type OptimisticConcurrencyStore struct {
	db *sql.DB
}

func NewOptimisticConcurrencyStore(db *sql.DB) *OptimisticConcurrencyStore {
	return &OptimisticConcurrencyStore{db: db}
}

func (s *OptimisticConcurrencyStore) UpdateWorkflowRun(ctx context.Context, update WorkflowRunUpdateCAS) error {
	result, err := s.db.ExecContext(
		ctx,
		`UPDATE workflow_runs
		 SET run_status = ?, plan_state = ?, updated_at = ?, row_version = row_version + 1
		 WHERE run_id = ? AND row_version = ?`,
		update.RunStatus,
		update.PlanState,
		update.UpdatedAt,
		update.RunID,
		update.ExpectedRowVersion,
	)
	if err != nil {
		return fmt.Errorf("update workflow run: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("workflow run rows affected: %w", err)
	}
	if rows == 1 {
		return nil
	}

	actual, found, err := s.workflowRunVersion(ctx, update.RunID)
	if err != nil {
		return err
	}
	if !found {
		return &EntityNotFoundError{EntityType: EntityTypeRun, RunID: update.RunID}
	}

	return &RowVersionConflictError{
		EntityType:      string(EntityTypeRun),
		EntityID:        update.RunID,
		ExpectedVersion: update.ExpectedRowVersion,
		ActualVersion:   actual,
	}
}

func (s *OptimisticConcurrencyStore) workflowRunVersion(ctx context.Context, runID string) (int64, bool, error) {
	var version int64
	err := s.db.QueryRowContext(ctx, `SELECT row_version FROM workflow_runs WHERE run_id = ?`, runID).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("load workflow run version: %w", err)
	}

	return version, true, nil
}

func (s *OptimisticConcurrencyStore) UpdateNodeRun(ctx context.Context, update NodeRunUpdateCAS) error {
	result, err := s.db.ExecContext(
		ctx,
		`UPDATE node_runs
		 SET node_status = ?, row_version = row_version + 1
		 WHERE node_run_id = ? AND run_id = ? AND row_version = ?`,
		update.NodeStatus,
		update.NodeRunID,
		update.RunID,
		update.ExpectedRowVersion,
	)
	if err != nil {
		return fmt.Errorf("update node run: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("node run rows affected: %w", err)
	}
	if rows == 1 {
		return nil
	}

	actual, found, err := s.nodeRunVersion(ctx, update.NodeRunID, update.RunID)
	if err != nil {
		return err
	}
	if !found {
		return &EntityNotFoundError{EntityType: EntityTypeNode, RunID: update.RunID, NodeRunID: update.NodeRunID}
	}

	return &RowVersionConflictError{
		EntityType:      string(EntityTypeNode),
		EntityID:        update.NodeRunID,
		ExpectedVersion: update.ExpectedRowVersion,
		ActualVersion:   actual,
	}
}

func (s *OptimisticConcurrencyStore) nodeRunVersion(ctx context.Context, nodeRunID, runID string) (int64, bool, error) {
	var version int64
	err := s.db.QueryRowContext(ctx, `SELECT row_version FROM node_runs WHERE node_run_id = ? AND run_id = ?`, nodeRunID, runID).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("load node run version: %w", err)
	}

	return version, true, nil
}
