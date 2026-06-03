package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type IdempotencyRecord struct {
	ID              string
	Route           string
	ActorID         string
	ClientKey       string
	RequestHash     string
	ResponseStatus  int
	ResponseHeaders string
	ResponseBody    string
	CreatedAt       string
	ExpiresAt       sql.NullString
}

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

func (s *IdempotencyStore) Lookup(ctx context.Context, route, actorID, clientKey string) (IdempotencyRecord, bool, error) {
	var record IdempotencyRecord
	err := s.db.QueryRowContext(
		ctx,
		`SELECT idempotency_key_id, route, actor_id, client_key, request_hash, response_status, response_headers_json, response_body_json, created_at, expires_at
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
		&record.ResponseStatus,
		&record.ResponseHeaders,
		&record.ResponseBody,
		&record.CreatedAt,
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

func (s *IdempotencyStore) Create(ctx context.Context, record IdempotencyRecord) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO idempotency_keys (idempotency_key_id, route, actor_id, client_key, request_hash, response_status, response_headers_json, response_body_json, created_at, expires_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ID,
		record.Route,
		record.ActorID,
		record.ClientKey,
		record.RequestHash,
		record.ResponseStatus,
		record.ResponseHeaders,
		record.ResponseBody,
		record.CreatedAt,
		record.ExpiresAt,
	)
	if err != nil {
		if isIdempotencyScopeUniqueViolation(err) {
			return &IdempotencyConflictError{
				Route:     record.Route,
				ActorID:   record.ActorID,
				ClientKey: record.ClientKey,
				Reason:    "scope already exists",
			}
		}
		return fmt.Errorf("create idempotency record: %w", err)
	}

	return nil
}

func isIdempotencyScopeUniqueViolation(err error) bool {
	return IsUniqueViolationOn(err,
		"idempotency_keys.route",
		"idempotency_keys.actor_id",
		"idempotency_keys.client_key",
	)
}
