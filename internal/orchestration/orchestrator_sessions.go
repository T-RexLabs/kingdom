package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"kingdom/internal/state"
)

func sessionIDForAttempt(attemptID string) string {
	trimmed := strings.TrimSpace(attemptID)
	if trimmed == "" {
		return ""
	}
	return fmt.Sprintf("session_%s", sanitizeFileToken(trimmed))
}

func (o *orchestrationLoop) preferredAttemptSessionID(ctx context.Context, attemptID string) string {
	trimmedAttemptID := strings.TrimSpace(attemptID)
	if trimmedAttemptID == "" {
		return ""
	}
	if o != nil && o.executionStore != nil {
		attempt, found, err := o.executionStore.GetAttempt(ctx, trimmedAttemptID)
		if err != nil {
			slog.Warn("failed to load attempt session identity", "attempt_id", trimmedAttemptID, "error", err)
		} else if found {
			if strings.TrimSpace(attempt.ProviderSessionID) != "" {
				return strings.TrimSpace(attempt.ProviderSessionID)
			}
			if strings.TrimSpace(attempt.SessionRef) != "" {
				return strings.TrimSpace(attempt.SessionRef)
			}
		}
	}
	return sessionIDForAttempt(trimmedAttemptID)
}

func (o *orchestrationLoop) ensureSessionLease(ctx context.Context, sessionID, agentID, now string) error {
	if o.sessions == nil {
		return nil
	}
	if strings.TrimSpace(agentID) == "" {
		agentID = deterministicAgentIDForRole("king")
	}

	nowTime, err := time.Parse(time.RFC3339, now)
	if err != nil {
		nowTime = o.now().UTC()
		now = nowTime.Format(time.RFC3339)
	}

	leaseExpiresAt := nowTime.Add(2 * time.Minute).Format(time.RFC3339)
	_, err = o.sessions.CreateLease(ctx, state.CreateSessionLeaseRequest{
		SessionID:       sessionID,
		AgentID:         agentID,
		LeaseExpiresAt:  leaseExpiresAt,
		LastHeartbeatAt: now,
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	if err == nil {
		return nil
	}

	if state.IsUniqueViolation(err) {
		_, renewErr := o.sessions.RenewLease(ctx, state.RenewSessionLeaseRequest{
			SessionID:       sessionID,
			LeaseExpiresAt:  leaseExpiresAt,
			LastHeartbeatAt: now,
			UpdatedAt:       now,
		})
		if renewErr == nil {
			return nil
		}
		var stateConflictErr *state.SessionLeaseStateConflictError
		if errors.As(renewErr, &stateConflictErr) {
			_, reactivateErr := o.sessions.ReactivateLease(ctx, state.ReactivateSessionLeaseRequest{
				SessionID:       sessionID,
				AgentID:         agentID,
				LeaseExpiresAt:  leaseExpiresAt,
				LastHeartbeatAt: now,
				UpdatedAt:       now,
			})
			if reactivateErr == nil {
				return nil
			}
			return fmt.Errorf("reactivate session lease: %w", reactivateErr)
		}
		return fmt.Errorf("renew session lease: %w", renewErr)
	}

	return fmt.Errorf("create session lease: %w", err)
}

func (o *orchestrationLoop) closeSessionLease(ctx context.Context, sessionID, now string) error {
	if o.sessions == nil {
		return nil
	}

	_, err := o.sessions.CloseLease(ctx, state.CloseSessionLeaseRequest{
		SessionID: sessionID,
		UpdatedAt: now,
	})
	if err == nil {
		return nil
	}

	var notFound *state.SessionLeaseNotFoundError
	if errors.As(err, &notFound) {
		return nil
	}
	if strings.Contains(err.Error(), "state conflict") {
		return nil
	}

	return err
}

func (o *orchestrationLoop) closeRunSessionLeases(ctx context.Context, runID, now string) error {
	sessionIDs, err := o.loadClosableRunSessionIDs(ctx, runID)
	if err != nil {
		return err
	}
	for _, sessionID := range sessionIDs {
		if closeErr := o.closeSessionLease(ctx, sessionID, now); closeErr != nil {
			return closeErr
		}
	}
	return nil
}

type executionCompletionFence struct {
	found        bool
	runStatus    string
	nodeStatus   string
	attemptState string
}

func (o *orchestrationLoop) loadExecutionCompletionFence(ctx context.Context, runID, nodeRunID string) (executionCompletionFence, error) {
	if o == nil || o.db == nil || strings.TrimSpace(runID) == "" || strings.TrimSpace(nodeRunID) == "" {
		return executionCompletionFence{found: true, runStatus: "running", nodeStatus: "running"}, nil
	}

	var fence executionCompletionFence
	err := o.db.QueryRowContext(ctx,
		`SELECT wr.run_status,
		        nr.node_status,
		        COALESCE(ae.state, '')
		 FROM workflow_runs wr
		 JOIN node_runs nr ON nr.run_id = wr.run_id
		 LEFT JOIN attempt_executions ae ON ae.run_id = nr.run_id AND ae.node_run_id = nr.node_run_id
		 WHERE wr.run_id = ? AND nr.node_run_id = ?
		 LIMIT 1`,
		runID,
		nodeRunID,
	).Scan(&fence.runStatus, &fence.nodeStatus, &fence.attemptState)
	if errors.Is(err, sql.ErrNoRows) {
		return executionCompletionFence{}, nil
	}
	if err != nil {
		return executionCompletionFence{}, fmt.Errorf("load execution completion fence: %w", err)
	}
	fence.found = true
	return fence, nil
}

func (o *orchestrationLoop) renewRunSessionLease(ctx context.Context, runID, now string) error {
	if o.sessions == nil {
		return nil
	}

	nowTime, err := time.Parse(time.RFC3339, now)
	if err != nil {
		nowTime = o.now().UTC()
		now = nowTime.Format(time.RFC3339)
	}

	sessionIDs, err := o.loadRenewableRunSessionIDs(ctx, runID)
	if err != nil {
		return err
	}
	for _, sessionID := range sessionIDs {
		_, renewErr := o.sessions.RenewLease(ctx, state.RenewSessionLeaseRequest{
			SessionID:       sessionID,
			LeaseExpiresAt:  nowTime.Add(2 * time.Minute).Format(time.RFC3339),
			LastHeartbeatAt: now,
			UpdatedAt:       now,
		})
		if renewErr == nil {
			continue
		}

		var notFoundErr *state.SessionLeaseNotFoundError
		if errors.As(renewErr, &notFoundErr) {
			continue
		}

		var conflictErr *state.SessionLeaseStateConflictError
		if errors.As(renewErr, &conflictErr) {
			continue
		}

		return renewErr
	}

	return nil
}

func (o *orchestrationLoop) loadClosableRunSessionIDs(ctx context.Context, runID string) ([]string, error) {
	prefix := fmt.Sprintf("session_%s_", sanitizeFileToken(runID))
	rows, err := o.db.QueryContext(ctx, `
SELECT DISTINCT ae.session_ref
FROM attempt_executions ae
WHERE ae.run_id = ?
  AND COALESCE(TRIM(ae.session_ref), '') <> ''
  AND ae.session_ref LIKE ?`, runID, prefix+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sessionIDs := make([]string, 0)
	for rows.Next() {
		var sessionID string
		if scanErr := rows.Scan(&sessionID); scanErr != nil {
			return nil, scanErr
		}
		sessionIDs = append(sessionIDs, strings.TrimSpace(sessionID))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sessionIDs, nil
}

func (o *orchestrationLoop) loadRenewableRunSessionIDs(ctx context.Context, runID string) ([]string, error) {
	prefix := fmt.Sprintf("session_%s_", sanitizeFileToken(runID))
	rows, err := o.db.QueryContext(ctx, `
SELECT DISTINCT sl.session_id
FROM session_leases sl
JOIN attempt_executions ae ON ae.session_ref = sl.session_id
WHERE ae.run_id = ?
  AND sl.session_id LIKE ?
  AND (
    (
      ae.state = 'starting'
      AND COALESCE(TRIM(ae.launch_started_at), '') <> ''
      AND COALESCE(TRIM(ae.started_at), '') = ''
    ) OR (
      ae.state = 'running'
      AND COALESCE(TRIM(ae.started_at), '') <> ''
    )
  )`, runID, prefix+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sessionIDs := make([]string, 0)
	for rows.Next() {
		var sessionID string
		if scanErr := rows.Scan(&sessionID); scanErr != nil {
			return nil, scanErr
		}
		sessionIDs = append(sessionIDs, strings.TrimSpace(sessionID))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sessionIDs, nil
}
