package state

import (
	"context"
	"fmt"
	"testing"
)

func TestIsUniqueViolationOnMatchesConstraintTargets(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	_, err := db.Exec(`INSERT INTO idempotency_keys (idempotency_key_id, route, actor_id, client_key, request_hash, response_status, response_headers_json, response_body_json, created_at, expires_at) VALUES ('idem_a', '/runs', 'actor_1', 'key_1', 'hash_1', 201, '{}', '{}', '2026-03-01T12:00:00Z', NULL)`)
	if err != nil {
		t.Fatalf("insert first idempotency key: %v", err)
	}
	_, err = db.Exec(`INSERT INTO idempotency_keys (idempotency_key_id, route, actor_id, client_key, request_hash, response_status, response_headers_json, response_body_json, created_at, expires_at) VALUES ('idem_b', '/runs', 'actor_1', 'key_1', 'hash_2', 201, '{}', '{}', '2026-03-01T12:01:00Z', NULL)`)
	if err == nil {
		t.Fatal("expected duplicate idempotency scope to fail")
	}

	if !IsUniqueViolation(err) {
		t.Fatalf("expected IsUniqueViolation to match duplicate scope error, got %v", err)
	}
	if !IsUniqueViolationOn(err, "idempotency_keys.route", "idempotency_keys.actor_id", "idempotency_keys.client_key") {
		t.Fatalf("expected IsUniqueViolationOn to match duplicate scope error, got %v", err)
	}
	if IsUniqueViolationOn(err, "idempotency_keys.idempotency_key_id") {
		t.Fatalf("expected scope conflict not to match primary-key target, got %v", err)
	}
}

func TestIsUniqueViolationOnDistinguishesPrimaryKeyCollisions(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	_, err := db.Exec(`INSERT INTO idempotency_keys (idempotency_key_id, route, actor_id, client_key, request_hash, response_status, response_headers_json, response_body_json, created_at, expires_at) VALUES ('idem_a', '/runs', 'actor_1', 'key_1', 'hash_1', 201, '{}', '{}', '2026-03-01T12:00:00Z', NULL)`)
	if err != nil {
		t.Fatalf("insert first idempotency key: %v", err)
	}
	_, err = db.Exec(`INSERT INTO idempotency_keys (idempotency_key_id, route, actor_id, client_key, request_hash, response_status, response_headers_json, response_body_json, created_at, expires_at) VALUES ('idem_a', '/other', 'actor_2', 'key_2', 'hash_2', 201, '{}', '{}', '2026-03-01T12:01:00Z', NULL)`)
	if err == nil {
		t.Fatal("expected duplicate idempotency primary key to fail")
	}

	if !IsUniqueViolation(err) {
		t.Fatalf("expected IsUniqueViolation to match primary-key conflict, got %v", err)
	}
	if IsUniqueViolationOn(err, "idempotency_keys.route", "idempotency_keys.actor_id", "idempotency_keys.client_key") {
		t.Fatalf("expected primary-key conflict not to match scope targets, got %v", err)
	}
	if !IsUniqueViolationOn(err, "idempotency_keys.idempotency_key_id") {
		t.Fatalf("expected primary-key conflict to match primary-key target, got %v", err)
	}
}

func TestIsSQLiteBusyMatchesBusyMarkers(t *testing.T) {
	t.Parallel()

	if !IsSQLiteBusy(fmt.Errorf("database is locked (5) (SQLITE_BUSY)")) {
		t.Fatal("expected SQLITE_BUSY marker to be recognized")
	}
	if IsSQLiteBusy(fmt.Errorf("constraint failed")) {
		t.Fatal("did not expect non-busy sqlite error to be classified as busy")
	}
}

func TestRetrySQLiteBusyRetriesBusyError(t *testing.T) {
	t.Parallel()

	attempts := 0
	err := retrySQLiteBusy(context.Background(), func() error {
		attempts++
		if attempts == 1 {
			return fmt.Errorf("database is locked (5) (SQLITE_BUSY)")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retry sqlite busy: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
}
