package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/sqlitex"
)

const (
	// Reconciliation is diagnostic only, so it must not turn one provider
	// export into an unbounded durable event stream.
	MaxPlanningActivityReconciliationEvents     = 128
	MaxPlanningActivityReconciliationEventBytes = 256 * 1024
)

const planningActivityReconciliationTimestampLayout = "2006-01-02T15:04:05.000000000Z07:00"

func createPlanningActivityReconciliationTx(ctx context.Context, tx *sql.Tx, operation PlanningOperation, createdAt string) error {
	invocationID := ""
	if operation.ActiveProviderInvocationID != nil {
		invocationID = strings.TrimSpace(*operation.ActiveProviderInvocationID)
	}
	if invocationID == "" {
		// State helpers also support runtime-owned test fixtures. Provider-backed
		// operations always have an invocation; without one there is no diagnostic
		// session to reconcile.
		return nil
	}
	createdAt, err := canonicalPlanningActivityReconciliationTimestamp(createdAt)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO planning_activity_reconciliations
		(planning_operation_id, planning_provider_invocation_id, status, lease_owner, lease_expires_at, lease_version, created_at, completed_at)
		VALUES (?, ?, 'pending', '', NULL, 0, ?, NULL)`, operation.OperationID, invocationID, createdAt)
	if err != nil {
		return fmt.Errorf("create planning activity reconciliation: %w", err)
	}
	return nil
}

func (s *PlanningStore) ClaimPlanningActivityReconciliation(ctx context.Context, operationID, leaseOwner, leaseExpiresAt, now string) (PlanningActivityReconciliation, bool, error) {
	operationID = strings.TrimSpace(operationID)
	leaseOwner = strings.TrimSpace(leaseOwner)
	if operationID == "" || leaseOwner == "" || strings.TrimSpace(leaseExpiresAt) == "" || strings.TrimSpace(now) == "" {
		return PlanningActivityReconciliation{}, false, fmt.Errorf("activity reconciliation operation and lease fields are required")
	}
	var err error
	leaseExpiresAt, err = canonicalPlanningActivityReconciliationTimestamp(leaseExpiresAt)
	if err != nil {
		return PlanningActivityReconciliation{}, false, err
	}
	now, err = canonicalPlanningActivityReconciliationTimestamp(now)
	if err != nil {
		return PlanningActivityReconciliation{}, false, err
	}
	var claimed PlanningActivityReconciliation
	claimedOK := false
	_, err = sqlitex.Retry(ctx, func() error {
		row := s.db.QueryRowContext(ctx, `UPDATE planning_activity_reconciliations
			SET lease_owner = ?, lease_expires_at = ?, lease_version = lease_version + 1
			WHERE planning_operation_id = ?
			  AND status = 'pending'
			  AND (TRIM(lease_owner) = '' OR lease_expires_at IS NULL OR lease_expires_at <= ?)
			RETURNING planning_operation_id, planning_provider_invocation_id, status, lease_owner, lease_expires_at, lease_version, created_at, completed_at`,
			leaseOwner, leaseExpiresAt, operationID, now)
		if err := scanPlanningActivityReconciliation(row, &claimed); err != nil {
			if err == sql.ErrNoRows {
				claimed = PlanningActivityReconciliation{}
				claimedOK = false
				return nil
			}
			return fmt.Errorf("claim planning activity reconciliation: %w", err)
		}
		claimedOK = true
		return nil
	}, sqlitex.Options{})
	if err != nil {
		return PlanningActivityReconciliation{}, false, err
	}
	return claimed, claimedOK, nil
}

func (s *PlanningStore) ListEligiblePlanningActivityReconciliations(ctx context.Context, now string, limit int) ([]PlanningActivityReconciliation, error) {
	return s.ListEligiblePlanningActivityReconciliationsPage(ctx, now, "", "", limit)
}

func (s *PlanningStore) ListEligiblePlanningActivityReconciliationsPage(ctx context.Context, now, afterCreatedAt, afterOperationID string, limit int) ([]PlanningActivityReconciliation, error) {
	if limit <= 0 {
		limit = DefaultPlanningOperationListLimit
	}
	var err error
	now, err = canonicalPlanningActivityReconciliationTimestamp(now)
	if err != nil {
		return nil, err
	}
	afterCreatedAt = strings.TrimSpace(afterCreatedAt)
	afterOperationID = strings.TrimSpace(afterOperationID)
	if afterCreatedAt != "" {
		afterCreatedAt, err = canonicalPlanningActivityReconciliationTimestamp(afterCreatedAt)
		if err != nil {
			return nil, err
		}
	}
	rows, err := s.db.QueryContext(ctx, `SELECT reconciliation.planning_operation_id, reconciliation.planning_provider_invocation_id, reconciliation.status, reconciliation.lease_owner, reconciliation.lease_expires_at, reconciliation.lease_version, reconciliation.created_at, reconciliation.completed_at
		FROM planning_activity_reconciliations reconciliation
		JOIN planning_operations operation ON operation.operation_id = reconciliation.planning_operation_id
		WHERE reconciliation.status = 'pending'
		  AND operation.status = 'succeeded'
		  AND (TRIM(reconciliation.lease_owner) = '' OR reconciliation.lease_expires_at IS NULL OR reconciliation.lease_expires_at <= ?)
		  AND (? = '' OR reconciliation.created_at > ? OR (reconciliation.created_at = ? AND reconciliation.planning_operation_id > ?))
		ORDER BY reconciliation.created_at ASC, reconciliation.planning_operation_id ASC
		LIMIT ?`, now, afterCreatedAt, afterCreatedAt, afterCreatedAt, afterOperationID, limit)
	if err != nil {
		return nil, fmt.Errorf("list eligible planning activity reconciliations: %w", err)
	}
	defer rows.Close()
	items := make([]PlanningActivityReconciliation, 0)
	for rows.Next() {
		var item PlanningActivityReconciliation
		if err := scanPlanningActivityReconciliation(rows, &item); err != nil {
			return nil, fmt.Errorf("scan planning activity reconciliation: %w", err)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *PlanningStore) PlanningActivityReconciliationPending(ctx context.Context, operationID string) (bool, error) {
	var status PlanningActivityReconciliationStatus
	err := s.db.QueryRowContext(ctx, `SELECT status FROM planning_activity_reconciliations WHERE planning_operation_id = ?`, strings.TrimSpace(operationID)).Scan(&status)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get planning activity reconciliation: %w", err)
	}
	return status == PlanningActivityReconciliationStatusPending, nil
}

func (s *PlanningStore) CompletePlanningActivityReconciliation(ctx context.Context, req CompletePlanningActivityReconciliationRequest) error {
	if strings.TrimSpace(req.PlanningOperationID) == "" || strings.TrimSpace(req.LeaseOwner) == "" || req.LeaseVersion < 1 || strings.TrimSpace(req.CompletedAt) == "" {
		return fmt.Errorf("activity reconciliation completion fields are required")
	}
	if err := validatePlanningOperationEventAppend(req.MarkerEvent); err != nil {
		return err
	}
	if strings.TrimSpace(req.MarkerEvent.OperationID) != strings.TrimSpace(req.PlanningOperationID) {
		return fmt.Errorf("activity reconciliation marker operation does not match reconciliation")
	}
	if len(req.ActivityEvents) > MaxPlanningActivityReconciliationEvents {
		return fmt.Errorf("reconciled activity exceeds %d events", MaxPlanningActivityReconciliationEvents)
	}
	activityBytes := 0
	for _, event := range req.ActivityEvents {
		if err := validatePlanningOperationEventAppend(event); err != nil {
			return err
		}
		if strings.TrimSpace(event.OperationID) != strings.TrimSpace(req.PlanningOperationID) {
			return fmt.Errorf("reconciled activity operation does not match reconciliation")
		}
		activityBytes += len(event.PayloadJSON)
		if activityBytes > MaxPlanningActivityReconciliationEventBytes {
			return fmt.Errorf("reconciled activity exceeds %d bytes", MaxPlanningActivityReconciliationEventBytes)
		}
	}
	completedAt, err := canonicalPlanningActivityReconciliationTimestamp(req.CompletedAt)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin complete planning activity reconciliation: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	result, err := tx.ExecContext(ctx, `UPDATE planning_activity_reconciliations
		SET status = 'completed', lease_owner = '', lease_expires_at = NULL, lease_version = lease_version + 1, completed_at = ?
		WHERE planning_operation_id = ? AND status = 'pending' AND lease_owner = ? AND lease_version = ?
		  AND lease_expires_at > ?
		  AND EXISTS (SELECT 1 FROM planning_operations WHERE operation_id = planning_activity_reconciliations.planning_operation_id AND status = 'succeeded')`,
		completedAt, strings.TrimSpace(req.PlanningOperationID), strings.TrimSpace(req.LeaseOwner), req.LeaseVersion, completedAt)
	if err != nil {
		return fmt.Errorf("complete planning activity reconciliation: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("complete planning activity reconciliation rows affected: %w", err)
	} else if affected != 1 {
		return ErrPlanningLeaseConflict
	}
	var nextSeq int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) + 1 FROM planning_operation_events WHERE operation_id = ?`, strings.TrimSpace(req.PlanningOperationID)).Scan(&nextSeq); err != nil {
		return fmt.Errorf("allocate reconciled activity event sequence: %w", err)
	}
	for _, event := range req.ActivityEvents {
		event.Seq = nextSeq
		event.EventID = fmt.Sprintf("%s:%d", strings.TrimSpace(req.PlanningOperationID), nextSeq)
		if _, err := insertPlanningOperationEvent(ctx, tx, event); err != nil {
			return fmt.Errorf("append reconciled activity: %w", err)
		}
		nextSeq++
	}
	marker := req.MarkerEvent
	marker.Seq = nextSeq
	marker.EventID = fmt.Sprintf("%s:%d", strings.TrimSpace(req.PlanningOperationID), nextSeq)
	if _, err := insertPlanningOperationEvent(ctx, tx, marker); err != nil {
		return fmt.Errorf("append activity reconciliation marker: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit planning activity reconciliation: %w", err)
	}
	committed = true
	return nil
}

func canonicalPlanningActivityReconciliationTimestamp(raw string) (string, error) {
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("parse activity reconciliation timestamp: %w", err)
	}
	return parsed.UTC().Format(planningActivityReconciliationTimestampLayout), nil
}

func scanPlanningActivityReconciliation(row scannable, dest *PlanningActivityReconciliation) error {
	var leaseExpiresAt, completedAt sql.NullString
	if err := row.Scan(&dest.PlanningOperationID, &dest.PlanningProviderInvocationID, &dest.Status, &dest.LeaseOwner, &leaseExpiresAt, &dest.LeaseVersion, &dest.CreatedAt, &completedAt); err != nil {
		return err
	}
	dest.LeaseExpiresAt = nullableStringFromSQL(leaseExpiresAt)
	dest.CompletedAt = nullableStringFromSQL(completedAt)
	return nil
}
