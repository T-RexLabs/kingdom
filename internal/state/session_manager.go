package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

type SessionLeaseStatus string

const (
	SessionLeaseStatusActive  SessionLeaseStatus = "active"
	SessionLeaseStatusExpired SessionLeaseStatus = "expired"
	SessionLeaseStatusClosed  SessionLeaseStatus = "closed"
)

type SessionLease struct {
	SessionID       string
	AgentID         string
	Status          SessionLeaseStatus
	LeaseExpiresAt  string
	LastHeartbeatAt string
	CreatedAt       string
	UpdatedAt       string
}

type SessionLeaseNotFoundError struct {
	SessionID string
}

func (e *SessionLeaseNotFoundError) Error() string {
	return fmt.Sprintf("session lease not found: %q", e.SessionID)
}

type SessionLeaseStateConflictError struct {
	SessionID       string
	ExpectedStatus  SessionLeaseStatus
	ObservedStatus  SessionLeaseStatus
	AttemptedAction string
}

func (e *SessionLeaseStateConflictError) Error() string {
	return fmt.Sprintf("session lease state conflict for %q during %s: expected %q got %q", e.SessionID, e.AttemptedAction, e.ExpectedStatus, e.ObservedStatus)
}

type SessionLeaseExpiredError struct {
	Code      string
	SessionID string
}

func (e *SessionLeaseExpiredError) Error() string {
	return fmt.Sprintf("%s: session lease expired for %q", e.Code, e.SessionID)
}

type SessionLifecycleMetrics struct {
	mu                    sync.Mutex
	createdCount          int
	renewedCount          int
	expiredCount          int
	closedCount           int
	recoveredCount        int
	staleRecoverySeconds  []float64
	lastExpiryReasonClass string
}

func NewSessionLifecycleMetrics() *SessionLifecycleMetrics {
	return &SessionLifecycleMetrics{}
}

func (m *SessionLifecycleMetrics) RecordCreated() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createdCount++
}

func (m *SessionLifecycleMetrics) RecordRenewed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.renewedCount++
}

func (m *SessionLifecycleMetrics) RecordExpired(reasonClass string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expiredCount++
	m.lastExpiryReasonClass = reasonClass
}

func (m *SessionLifecycleMetrics) RecordClosed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closedCount++
}

func (m *SessionLifecycleMetrics) RecordRecovered() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recoveredCount++
}

func (m *SessionLifecycleMetrics) ObserveStaleRecovery(seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.staleRecoverySeconds = append(m.staleRecoverySeconds, seconds)
}

func (m *SessionLifecycleMetrics) Snapshot() SessionLifecycleMetricsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	staleSamples := make([]float64, len(m.staleRecoverySeconds))
	copy(staleSamples, m.staleRecoverySeconds)

	return SessionLifecycleMetricsSnapshot{
		CreatedCount:          m.createdCount,
		RenewedCount:          m.renewedCount,
		ExpiredCount:          m.expiredCount,
		ClosedCount:           m.closedCount,
		RecoveredCount:        m.recoveredCount,
		StaleRecoverySeconds:  staleSamples,
		LastExpiryReasonClass: m.lastExpiryReasonClass,
	}
}

type SessionLifecycleMetricsSnapshot struct {
	CreatedCount          int
	RenewedCount          int
	ExpiredCount          int
	ClosedCount           int
	RecoveredCount        int
	StaleRecoverySeconds  []float64
	LastExpiryReasonClass string
}

type SessionMetricsRecorder interface {
	RecordCreated()
	RecordRenewed()
	RecordExpired(reasonClass string)
	RecordClosed()
	RecordRecovered()
	ObserveStaleRecovery(seconds float64)
}

type CreateSessionLeaseRequest struct {
	SessionID       string
	AgentID         string
	LeaseExpiresAt  string
	LastHeartbeatAt string
	CreatedAt       string
	UpdatedAt       string
}

type RenewSessionLeaseRequest struct {
	SessionID       string
	LeaseExpiresAt  string
	LastHeartbeatAt string
	UpdatedAt       string
}

type ReactivateSessionLeaseRequest struct {
	SessionID       string
	AgentID         string
	LeaseExpiresAt  string
	LastHeartbeatAt string
	UpdatedAt       string
}

type ExpireSessionLeaseRequest struct {
	SessionID      string
	LeaseExpiresAt string
	UpdatedAt      string
	ReasonClass    string
}

type CloseSessionLeaseRequest struct {
	SessionID string
	UpdatedAt string
}

type FindStaleSessionLeasesRequest struct {
	Now                     string
	StaleHeartbeatThreshold time.Duration
}

type SessionManager struct {
	db      *sql.DB
	metrics SessionMetricsRecorder
}

func NewSessionManager(db *sql.DB, metrics SessionMetricsRecorder) *SessionManager {
	if metrics == nil {
		metrics = NewSessionLifecycleMetrics()
	}
	return &SessionManager{db: db, metrics: metrics}
}

func (m *SessionManager) CreateLease(ctx context.Context, req CreateSessionLeaseRequest) (SessionLease, error) {
	if err := validateCreateLeaseRequest(req); err != nil {
		return SessionLease{}, err
	}

	lease := SessionLease{
		SessionID:       req.SessionID,
		AgentID:         req.AgentID,
		Status:          SessionLeaseStatusActive,
		LeaseExpiresAt:  req.LeaseExpiresAt,
		LastHeartbeatAt: req.LastHeartbeatAt,
		CreatedAt:       req.CreatedAt,
		UpdatedAt:       req.UpdatedAt,
	}

	_, err := m.db.ExecContext(ctx,
		`INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		lease.SessionID,
		lease.AgentID,
		lease.Status,
		lease.LeaseExpiresAt,
		lease.LastHeartbeatAt,
		lease.CreatedAt,
		lease.UpdatedAt,
	)
	if err != nil {
		return SessionLease{}, fmt.Errorf("insert session lease: %w", err)
	}

	m.metrics.RecordCreated()
	return lease, nil
}

func (m *SessionManager) RenewLease(ctx context.Context, req RenewSessionLeaseRequest) (SessionLease, error) {
	if err := validateRenewLeaseRequest(req); err != nil {
		return SessionLease{}, err
	}

	result, err := m.db.ExecContext(ctx,
		`UPDATE session_leases
		 SET lease_expires_at = ?, last_heartbeat_at = ?, updated_at = ?
		 WHERE session_id = ? AND status = 'active'`,
		req.LeaseExpiresAt,
		req.LastHeartbeatAt,
		req.UpdatedAt,
		req.SessionID,
	)
	if err != nil {
		return SessionLease{}, fmt.Errorf("renew session lease: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return SessionLease{}, fmt.Errorf("session lease rows affected on renew: %w", err)
	}
	if rows == 0 {
		return SessionLease{}, m.leaseMutationConflict(ctx, req.SessionID, SessionLeaseStatusActive, "renew")
	}

	lease, err := m.GetLease(ctx, req.SessionID)
	if err != nil {
		return SessionLease{}, err
	}

	m.metrics.RecordRenewed()
	return lease, nil
}

func (m *SessionManager) ReactivateLease(ctx context.Context, req ReactivateSessionLeaseRequest) (SessionLease, error) {
	if err := validateReactivateLeaseRequest(req); err != nil {
		return SessionLease{}, err
	}

	result, err := m.db.ExecContext(ctx,
		`UPDATE session_leases
		 SET status = 'active', agent_id = ?, lease_expires_at = ?, last_heartbeat_at = ?, updated_at = ?
		 WHERE session_id = ? AND status IN ('closed', 'expired')`,
		req.AgentID,
		req.LeaseExpiresAt,
		req.LastHeartbeatAt,
		req.UpdatedAt,
		req.SessionID,
	)
	if err != nil {
		return SessionLease{}, fmt.Errorf("reactivate session lease: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return SessionLease{}, fmt.Errorf("session lease rows affected on reactivate: %w", err)
	}
	if rows == 0 {
		return SessionLease{}, m.leaseMutationConflict(ctx, req.SessionID, SessionLeaseStatusClosed, "reactivate")
	}

	lease, err := m.GetLease(ctx, req.SessionID)
	if err != nil {
		return SessionLease{}, err
	}

	m.metrics.RecordRenewed()
	return lease, nil
}

func (m *SessionManager) ExpireLease(ctx context.Context, req ExpireSessionLeaseRequest) (SessionLease, error) {
	if err := validateExpireLeaseRequest(req); err != nil {
		return SessionLease{}, err
	}

	result, err := m.db.ExecContext(ctx,
		`UPDATE session_leases
		 SET status = 'expired', lease_expires_at = ?, updated_at = ?
		 WHERE session_id = ? AND status = 'active'`,
		req.LeaseExpiresAt,
		req.UpdatedAt,
		req.SessionID,
	)
	if err != nil {
		return SessionLease{}, fmt.Errorf("expire session lease: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return SessionLease{}, fmt.Errorf("session lease rows affected on expire: %w", err)
	}
	if rows == 0 {
		return SessionLease{}, m.leaseMutationConflict(ctx, req.SessionID, SessionLeaseStatusActive, "expire")
	}

	lease, err := m.GetLease(ctx, req.SessionID)
	if err != nil {
		return SessionLease{}, err
	}

	m.metrics.RecordExpired(req.ReasonClass)
	return lease, &SessionLeaseExpiredError{Code: "session_lease_expired", SessionID: req.SessionID}
}

func (m *SessionManager) CloseLease(ctx context.Context, req CloseSessionLeaseRequest) (SessionLease, error) {
	if err := validateCloseLeaseRequest(req); err != nil {
		return SessionLease{}, err
	}

	result, err := m.db.ExecContext(ctx,
		`UPDATE session_leases
		 SET status = 'closed', updated_at = ?
		 WHERE session_id = ? AND status IN ('active', 'expired')`,
		req.UpdatedAt,
		req.SessionID,
	)
	if err != nil {
		return SessionLease{}, fmt.Errorf("close session lease: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return SessionLease{}, fmt.Errorf("session lease rows affected on close: %w", err)
	}
	if rows == 0 {
		lease, getErr := m.GetLease(ctx, req.SessionID)
		if getErr != nil {
			return SessionLease{}, getErr
		}
		return SessionLease{}, &SessionLeaseStateConflictError{
			SessionID:       req.SessionID,
			ExpectedStatus:  SessionLeaseStatusActive,
			ObservedStatus:  lease.Status,
			AttemptedAction: "close",
		}
	}

	lease, err := m.GetLease(ctx, req.SessionID)
	if err != nil {
		return SessionLease{}, err
	}

	m.metrics.RecordClosed()
	return lease, nil
}

func (m *SessionManager) FindStaleLeases(ctx context.Context, req FindStaleSessionLeasesRequest) ([]SessionLease, error) {
	nowTime, threshold, err := normalizeStaleLeaseInputs(req)
	if err != nil {
		return nil, err
	}

	cutoff := nowTime.Add(-threshold).UTC().Format(time.RFC3339)
	query := fmt.Sprintf(`SELECT session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at
		 FROM session_leases
		 WHERE status = 'active'
		   AND %s
		 ORDER BY session_id ASC`, staleActiveSessionLeaseConditionSQL("session_leases"))

	rows, err := m.db.QueryContext(ctx,
		query,
		cutoff,
		nowTime.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return nil, fmt.Errorf("find stale session leases: %w", err)
	}
	defer rows.Close()

	leases := make([]SessionLease, 0)
	for rows.Next() {
		var lease SessionLease
		if err := rows.Scan(
			&lease.SessionID,
			&lease.AgentID,
			&lease.Status,
			&lease.LeaseExpiresAt,
			&lease.LastHeartbeatAt,
			&lease.CreatedAt,
			&lease.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan stale session lease: %w", err)
		}
		leases = append(leases, lease)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stale session leases: %w", err)
	}

	return leases, nil
}

func staleActiveSessionLeaseConditionSQL(tableAlias string) string {
	trimmedAlias := tableAlias
	if trimmedAlias == "" {
		trimmedAlias = "session_leases"
	}
	return fmt.Sprintf(`(
		COALESCE(TRIM(%[1]s.last_heartbeat_at), '') = '' OR
		%[1]s.last_heartbeat_at <= ? OR
		COALESCE(TRIM(%[1]s.lease_expires_at), '') = '' OR
		%[1]s.lease_expires_at <= ?
	)`, trimmedAlias)
}

func (m *SessionManager) ReconcileStaleLeases(ctx context.Context, req FindStaleSessionLeasesRequest) ([]SessionLease, error) {
	nowTime, _, err := normalizeStaleLeaseInputs(req)
	if err != nil {
		return nil, err
	}

	staleLeases, err := m.FindStaleLeases(ctx, req)
	if err != nil {
		return nil, err
	}

	recovered := make([]SessionLease, 0, len(staleLeases))
	for _, lease := range staleLeases {
		expired, expireErr := m.ExpireLease(ctx, ExpireSessionLeaseRequest{
			SessionID:      lease.SessionID,
			LeaseExpiresAt: nowTime.UTC().Format(time.RFC3339),
			UpdatedAt:      nowTime.UTC().Format(time.RFC3339),
			ReasonClass:    "stale_heartbeat",
		})
		if expireErr != nil {
			var leaseExpiredErr *SessionLeaseExpiredError
			if !errors.As(expireErr, &leaseExpiredErr) {
				return nil, expireErr
			}
		}

		heartbeatAt, parseErr := time.Parse(time.RFC3339, lease.LastHeartbeatAt)
		if parseErr == nil {
			m.metrics.ObserveStaleRecovery(nowTime.Sub(heartbeatAt).Seconds())
		}
		m.metrics.RecordRecovered()
		recovered = append(recovered, expired)
	}

	return recovered, nil
}

func (m *SessionManager) GetLease(ctx context.Context, sessionID string) (SessionLease, error) {
	var lease SessionLease
	err := m.db.QueryRowContext(ctx,
		`SELECT session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at
		 FROM session_leases
		 WHERE session_id = ?`,
		sessionID,
	).Scan(
		&lease.SessionID,
		&lease.AgentID,
		&lease.Status,
		&lease.LeaseExpiresAt,
		&lease.LastHeartbeatAt,
		&lease.CreatedAt,
		&lease.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SessionLease{}, &SessionLeaseNotFoundError{SessionID: sessionID}
	}
	if err != nil {
		return SessionLease{}, fmt.Errorf("get session lease %q: %w", sessionID, err)
	}

	return lease, nil
}

func (m *SessionManager) leaseMutationConflict(ctx context.Context, sessionID string, expectedStatus SessionLeaseStatus, attemptedAction string) error {
	lease, err := m.GetLease(ctx, sessionID)
	if err != nil {
		return err
	}
	return &SessionLeaseStateConflictError{
		SessionID:       sessionID,
		ExpectedStatus:  expectedStatus,
		ObservedStatus:  lease.Status,
		AttemptedAction: attemptedAction,
	}
}

func normalizeStaleLeaseInputs(req FindStaleSessionLeasesRequest) (time.Time, time.Duration, error) {
	now, err := time.Parse(time.RFC3339, req.Now)
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("parse now timestamp: %w", err)
	}

	threshold := req.StaleHeartbeatThreshold
	if threshold <= 0 {
		threshold = DefaultStaleHeartbeatThreshold
	}

	return now.UTC(), threshold, nil
}

func validateCreateLeaseRequest(req CreateSessionLeaseRequest) error {
	if req.SessionID == "" {
		return fmt.Errorf("session id is required")
	}
	if req.AgentID == "" {
		return fmt.Errorf("agent id is required")
	}
	if req.LeaseExpiresAt == "" {
		return fmt.Errorf("lease expiration timestamp is required")
	}
	if req.LastHeartbeatAt == "" {
		return fmt.Errorf("last heartbeat timestamp is required")
	}
	if req.CreatedAt == "" {
		return fmt.Errorf("created timestamp is required")
	}
	if req.UpdatedAt == "" {
		return fmt.Errorf("updated timestamp is required")
	}
	return nil
}

func validateRenewLeaseRequest(req RenewSessionLeaseRequest) error {
	if req.SessionID == "" {
		return fmt.Errorf("session id is required")
	}
	if req.LeaseExpiresAt == "" {
		return fmt.Errorf("lease expiration timestamp is required")
	}
	if req.LastHeartbeatAt == "" {
		return fmt.Errorf("last heartbeat timestamp is required")
	}
	if req.UpdatedAt == "" {
		return fmt.Errorf("updated timestamp is required")
	}
	return nil
}

func validateReactivateLeaseRequest(req ReactivateSessionLeaseRequest) error {
	if req.SessionID == "" {
		return fmt.Errorf("session id is required")
	}
	if req.AgentID == "" {
		return fmt.Errorf("agent id is required")
	}
	if req.LeaseExpiresAt == "" {
		return fmt.Errorf("lease expiration timestamp is required")
	}
	if req.LastHeartbeatAt == "" {
		return fmt.Errorf("last heartbeat timestamp is required")
	}
	if req.UpdatedAt == "" {
		return fmt.Errorf("updated timestamp is required")
	}
	return nil
}

func validateExpireLeaseRequest(req ExpireSessionLeaseRequest) error {
	if req.SessionID == "" {
		return fmt.Errorf("session id is required")
	}
	if req.LeaseExpiresAt == "" {
		return fmt.Errorf("lease expiration timestamp is required")
	}
	if req.UpdatedAt == "" {
		return fmt.Errorf("updated timestamp is required")
	}
	if req.ReasonClass == "" {
		return fmt.Errorf("reason class is required")
	}
	return nil
}

func validateCloseLeaseRequest(req CloseSessionLeaseRequest) error {
	if req.SessionID == "" {
		return fmt.Errorf("session id is required")
	}
	if req.UpdatedAt == "" {
		return fmt.Errorf("updated timestamp is required")
	}
	return nil
}
