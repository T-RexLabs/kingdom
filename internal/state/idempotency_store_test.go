package state

import (
	"context"
	"errors"
	"testing"
)

func TestIdempotencyStoreCreateAndLookup(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	store := NewIdempotencyStore(db)
	record := IdempotencyRecord{
		ID:              "idem_a",
		Route:           "/runs",
		ActorID:         "actor_1",
		ClientKey:       "key_1",
		RequestHash:     "h_1",
		ResponseStatus:  201,
		ResponseHeaders: `{"Content-Type":"application/json"}`,
		ResponseBody:    `{"run_id":"run_1"}`,
		CreatedAt:       "2026-03-01T12:00:00Z",
	}

	if err := store.Create(context.Background(), record); err != nil {
		t.Fatalf("create idempotency record: %v", err)
	}

	got, found, err := store.Lookup(context.Background(), record.Route, record.ActorID, record.ClientKey)
	if err != nil {
		t.Fatalf("lookup idempotency record: %v", err)
	}
	if !found {
		t.Fatal("expected record to be found")
	}
	if got.RequestHash != record.RequestHash {
		t.Fatalf("request hash mismatch, got %q", got.RequestHash)
	}
	if got.ResponseStatus != record.ResponseStatus {
		t.Fatalf("response status mismatch, got %d", got.ResponseStatus)
	}
	if got.ResponseBody != record.ResponseBody {
		t.Fatalf("response body mismatch, got %s", got.ResponseBody)
	}
}

func TestIdempotencyStoreDuplicateScopeConflict(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	store := NewIdempotencyStore(db)
	first := IdempotencyRecord{
		ID:              "idem_a",
		Route:           "/runs",
		ActorID:         "actor_1",
		ClientKey:       "key_1",
		RequestHash:     "h_1",
		ResponseStatus:  201,
		ResponseHeaders: `{"Content-Type":"application/json"}`,
		ResponseBody:    `{"run_id":"run_1"}`,
		CreatedAt:       "2026-03-01T12:00:00Z",
	}
	second := first
	second.ID = "idem_b"
	second.RequestHash = "h_2"

	if err := store.Create(context.Background(), first); err != nil {
		t.Fatalf("create first idempotency record: %v", err)
	}

	err := store.Create(context.Background(), second)
	if err == nil {
		t.Fatal("expected duplicate scope to fail")
	}

	var conflictErr *IdempotencyConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected IdempotencyConflictError, got %T: %v", err, err)
	}
}
