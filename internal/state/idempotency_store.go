package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"kingdom/internal/idempotency"
)

type IdempotencyRecord struct {
	ID              string
	Route           string
	ActorID         string
	ClientKey       string
	RequestHash     string
	Status          string
	ResponseStatus  int
	ResponseHeaders string
	ResponseBody    string
	CreatedAt       string
	UpdatedAt       string
	ExpiresAt       sql.NullString
}

const (
	IdempotencyStatusPending   = "pending"
	IdempotencyStatusCompleted = "completed"
)

type IdempotencyConflictError struct {
	Route     string
	ActorID   string
	ClientKey string
	Reason    string
}

func (e *IdempotencyConflictError) Error() string {
	return fmt.Sprintf(
		"idempotency conflict for route=%q actor=%q client_key=%q: %s",
		e.Route,
		e.ActorID,
		e.ClientKey,
		e.Reason,
	)
}

type IdempotencyStore struct {
	db *sql.DB
}

func NewIdempotencyStore(db *sql.DB) *IdempotencyStore {
	return &IdempotencyStore{db: db}
}

// ReserveVersioned atomically advances the durable clock, rejects expired
// keys, prunes expired completed rows, and reserves an admissible mutation.
func (s *IdempotencyStore) ReserveVersioned(ctx context.Context, record IdempotencyRecord, key idempotency.Key, now time.Time) (bool, time.Time, error) {
	if s == nil || s.db == nil {
		return false, time.Time{}, fmt.Errorf("idempotency store database is required")
	}
	record.ClientKey = key.Raw
	record.ExpiresAt = sql.NullString{String: key.ExpiresAt.UTC().Format(time.RFC3339), Valid: true}
	now = now.UTC().Truncate(time.Second)
	nowUnix := now.Unix()
	if nowUnix < 0 {
		nowUnix = 0
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, time.Time{}, fmt.Errorf("begin idempotency reservation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO idempotency_retention_clock (singleton, high_water_unix)
		 VALUES (1, ?)
		 ON CONFLICT(singleton) DO UPDATE SET high_water_unix = MAX(high_water_unix, excluded.high_water_unix)`,
		nowUnix,
	); err != nil {
		return false, time.Time{}, fmt.Errorf("advance idempotency retention clock: %w", err)
	}
	var highWaterUnix int64
	if err := tx.QueryRowContext(ctx,
		`SELECT high_water_unix FROM idempotency_retention_clock WHERE singleton = 1`,
	).Scan(&highWaterUnix); err != nil {
		return false, time.Time{}, fmt.Errorf("read idempotency retention clock: %w", err)
	}
	highWater := time.Unix(highWaterUnix, 0).UTC()
	pendingExists := false
	var existingStatus string
	err = tx.QueryRowContext(ctx,
		`SELECT status FROM idempotency_keys WHERE route = ? AND actor_id = ? AND client_key = ?`,
		record.Route,
		record.ActorID,
		key.Raw,
	).Scan(&existingStatus)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, time.Time{}, fmt.Errorf("inspect idempotency reservation: %w", err)
	}
	pendingExists = err == nil && existingStatus == IdempotencyStatusPending
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM idempotency_keys
		 WHERE expires_at IS NOT NULL
		   AND status = ?
		   AND expires_at <= ?`,
		IdempotencyStatusCompleted,
		highWater.Format(time.RFC3339),
	); err != nil {
		return false, time.Time{}, fmt.Errorf("prune expired idempotency keys: %w", err)
	}
	if pendingExists {
		var retained int
		if err := tx.QueryRowContext(ctx,
			`SELECT EXISTS(SELECT 1 FROM idempotency_keys WHERE route = ? AND actor_id = ? AND client_key = ? AND status = ?)`,
			record.Route,
			record.ActorID,
			key.Raw,
			IdempotencyStatusPending,
		).Scan(&retained); err != nil {
			return false, time.Time{}, fmt.Errorf("inspect retained idempotency reservation: %w", err)
		}
		if retained == 1 {
			if err := tx.Commit(); err != nil {
				return false, time.Time{}, fmt.Errorf("commit idempotency pending transaction: %w", err)
			}
			return false, highWater, nil
		}
	}
	if validationErr := key.ValidateAt(highWater); validationErr != nil {
		if err := tx.Commit(); err != nil {
			return false, time.Time{}, fmt.Errorf("commit idempotency retention transaction: %w", err)
		}
		return false, highWater, validationErr
	}
	result, err := tx.ExecContext(ctx,
		`INSERT INTO idempotency_keys (
			idempotency_key_id, route, actor_id, client_key, request_hash, status,
			response_status, response_headers_json, response_body_json,
			created_at, updated_at, expires_at
		) VALUES (?, ?, ?, ?, ?, 'pending', 0, '{}', '', ?, ?, ?)
		ON CONFLICT(route, actor_id, client_key) DO NOTHING`,
		record.ID,
		record.Route,
		record.ActorID,
		record.ClientKey,
		record.RequestHash,
		record.CreatedAt,
		record.UpdatedAt,
		record.ExpiresAt,
	)
	if err != nil {
		return false, time.Time{}, fmt.Errorf("reserve idempotency record: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, time.Time{}, fmt.Errorf("read idempotency reservation result: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, time.Time{}, fmt.Errorf("commit idempotency reservation transaction: %w", err)
	}
	return rows == 1, highWater, nil
}

func (s *IdempotencyStore) Lookup(ctx context.Context, route, actorID, clientKey string) (IdempotencyRecord, bool, error) {
	var record IdempotencyRecord
	err := s.db.QueryRowContext(
		ctx,
		`SELECT idempotency_key_id, route, actor_id, client_key, request_hash, status, response_status, response_headers_json, response_body_json, created_at, updated_at, expires_at
		 FROM idempotency_keys
		 WHERE route = ? AND actor_id = ? AND client_key = ?`,
		route,
		actorID,
		clientKey,
	).Scan(
		&record.ID,
		&record.Route,
		&record.ActorID,
		&record.ClientKey,
		&record.RequestHash,
		&record.Status,
		&record.ResponseStatus,
		&record.ResponseHeaders,
		&record.ResponseBody,
		&record.CreatedAt,
		&record.UpdatedAt,
		&record.ExpiresAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return IdempotencyRecord{}, false, nil
	}
	if err != nil {
		return IdempotencyRecord{}, false, fmt.Errorf("lookup idempotency record: %w", err)
	}

	return record, true, nil
}

// Finalize atomically changes the owner's pending reservation to completed.
func (s *IdempotencyStore) Finalize(ctx context.Context, record IdempotencyRecord) error {
	result, err := s.db.ExecContext(
		ctx,
		`UPDATE idempotency_keys
		 SET status = 'completed', response_status = ?, response_headers_json = ?,
		     response_body_json = ?, updated_at = ?
		 WHERE idempotency_key_id = ? AND request_hash = ? AND status = 'pending'`,
		record.ResponseStatus,
		record.ResponseHeaders,
		record.ResponseBody,
		record.UpdatedAt,
		record.ID,
		record.RequestHash,
	)
	if err != nil {
		return fmt.Errorf("finalize idempotency record: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read idempotency finalization result: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("finalize idempotency record: pending reservation not found")
	}
	return nil
}
