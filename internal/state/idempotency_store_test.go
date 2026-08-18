package state

import (
	"bytes"
	"context"
	"testing"
	"time"

	"kingdom/internal/dbmigrations"
	"kingdom/internal/idempotency"
)

func TestIdempotencyStoreReserveFinalizeAndLookup(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	store := NewIdempotencyStore(db)
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	rawKey, err := idempotency.GenerateWithReader("run-start", now, idempotency.DefaultTTL, bytes.NewReader(make([]byte, 16)))
	if err != nil {
		t.Fatal(err)
	}
	key, err := idempotency.Parse(rawKey)
	if err != nil {
		t.Fatal(err)
	}
	record := IdempotencyRecord{
		ID:              "idem_a",
		Route:           "/runs",
		ActorID:         "actor_1",
		ClientKey:       rawKey,
		RequestHash:     "h_1",
		ResponseStatus:  201,
		ResponseHeaders: `{"Content-Type":"application/json"}`,
		ResponseBody:    `{"run_id":"run_1"}`,
		CreatedAt:       now.Format(time.RFC3339),
		UpdatedAt:       now.Format(time.RFC3339),
	}

	owner, _, err := store.ReserveVersioned(context.Background(), record, key, now)
	if err != nil || !owner {
		t.Fatalf("reserve idempotency record: owner=%v err=%v", owner, err)
	}
	if err := store.Finalize(context.Background(), record); err != nil {
		t.Fatalf("finalize idempotency record: %v", err)
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

func TestIdempotencyStoreDuplicateScopeDoesNotTakeOwnership(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	store := NewIdempotencyStore(db)
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	rawKey, err := idempotency.GenerateWithReader("run-start", now, idempotency.DefaultTTL, bytes.NewReader(make([]byte, 16)))
	if err != nil {
		t.Fatal(err)
	}
	key, err := idempotency.Parse(rawKey)
	if err != nil {
		t.Fatal(err)
	}
	first := IdempotencyRecord{
		ID:              "idem_a",
		Route:           "/runs",
		ActorID:         "actor_1",
		ClientKey:       rawKey,
		RequestHash:     "h_1",
		ResponseStatus:  201,
		ResponseHeaders: `{"Content-Type":"application/json"}`,
		ResponseBody:    `{"run_id":"run_1"}`,
		CreatedAt:       now.Format(time.RFC3339),
		UpdatedAt:       now.Format(time.RFC3339),
	}
	second := first
	second.ID = "idem_b"
	second.RequestHash = "h_2"

	owner, _, err := store.ReserveVersioned(context.Background(), first, key, now)
	if err != nil || !owner {
		t.Fatalf("reserve first idempotency record: owner=%v err=%v", owner, err)
	}

	owner, _, err = store.ReserveVersioned(context.Background(), second, key, now)
	if err != nil {
		t.Fatal(err)
	}
	if owner {
		t.Fatal("duplicate scope took ownership")
	}
}

func TestIdempotencyStoreRetentionClockDoesNotMoveBackwardAndPrunes(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	applyMigration(t, db, "0004_bound_idempotency_retention.up.sql")
	if _, err := db.Exec(`UPDATE idempotency_retention_clock SET high_water_unix = 0 WHERE singleton = 1`); err != nil {
		t.Fatal(err)
	}
	store := NewIdempotencyStore(db)
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	insert := func(id, status string, expiresAt time.Time) {
		t.Helper()
		_, err := db.Exec(`INSERT INTO idempotency_keys (
			idempotency_key_id, route, actor_id, client_key, request_hash, status,
			response_status, response_headers_json, response_body_json, created_at, updated_at, expires_at
		) VALUES (?, '/runs', 'actor_1', ?, ?, ?, 0, '{}', '{}', ?, ?, ?)`,
			id, id, id, status, now.Add(-2*time.Hour).Format(time.RFC3339), now.Add(-2*time.Hour).Format(time.RFC3339), expiresAt.Format(time.RFC3339))
		if err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}
	insert("completed_expired", IdempotencyStatusCompleted, now.Add(-time.Minute))
	insert("pending_recent", IdempotencyStatusPending, now.Add(-30*time.Minute))
	insert("pending_abandoned", IdempotencyStatusPending, now.Add(-2*time.Hour))
	insert("completed_live", IdempotencyStatusCompleted, now.Add(time.Hour))

	rawKey, err := idempotency.GenerateWithReader("test", now, idempotency.DefaultTTL, bytes.NewReader(make([]byte, 16)))
	if err != nil {
		t.Fatal(err)
	}
	key, err := idempotency.Parse(rawKey)
	if err != nil {
		t.Fatal(err)
	}
	clockRecord := IdempotencyRecord{
		ID: "clock", Route: "/clock", ActorID: "actor_1", ClientKey: rawKey,
		RequestHash: "clock", CreatedAt: now.Format(time.RFC3339), UpdatedAt: now.Format(time.RFC3339),
	}
	owner, highWater, err := store.ReserveVersioned(context.Background(), clockRecord, key, now)
	if err != nil {
		t.Fatal(err)
	}
	if !owner {
		t.Fatal("first clock reservation was not owned")
	}
	if !highWater.Equal(now) {
		t.Fatalf("high water=%s want %s", highWater, now)
	}
	owner, highWater, err = store.ReserveVersioned(context.Background(), clockRecord, key, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if owner {
		t.Fatal("duplicate clock reservation was owned")
	}
	if !highWater.Equal(now) {
		t.Fatalf("clock moved backward to %s", highWater)
	}

	for id, wantFound := range map[string]bool{
		"completed_expired": false,
		"pending_recent":    true,
		"pending_abandoned": false,
		"completed_live":    true,
	} {
		_, found, err := store.Lookup(context.Background(), "/runs", "actor_1", id)
		if err != nil {
			t.Fatal(err)
		}
		if found != wantFound {
			t.Fatalf("%s found=%v want %v", id, found, wantFound)
		}
	}
}

func TestIdempotencyRetentionMigrationPreservesExistingEvidence(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	baseline, err := dbmigrations.Read("0001_initial_schema.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(baseline)); err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO idempotency_keys (
		idempotency_key_id, route, actor_id, client_key, request_hash, status,
		response_status, response_headers_json, response_body_json, created_at, updated_at, expires_at
	) VALUES ('legacy', '/runs', 'actor', 'kd1.1.60.test.AAAAAAAAAAAAAAAAAAAAAA', 'hash', 'pending', 0, '{}', '', '2026-08-17T12:00:00Z', '2026-08-17T12:00:00Z', NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	applyMigration(t, db, "0004_bound_idempotency_retention.up.sql")
	record, found, err := NewIdempotencyStore(db).Lookup(context.Background(), "/runs", "actor", "kd1.1.60.test.AAAAAAAAAAAAAAAAAAAAAA")
	if err != nil {
		t.Fatal(err)
	}
	if !found || !record.ExpiresAt.Valid || record.ExpiresAt.String == "" {
		t.Fatalf("existing evidence was not retained with bounded expiry: %#v", record)
	}
	var highWaterUnix int64
	if err := db.QueryRow(`SELECT high_water_unix FROM idempotency_retention_clock WHERE singleton = 1`).Scan(&highWaterUnix); err != nil {
		t.Fatal(err)
	}
	if highWaterUnix <= 0 {
		t.Fatalf("migration high-water clock=%d", highWaterUnix)
	}
	expiresAt, err := time.Parse(time.RFC3339, record.ExpiresAt.String)
	if err != nil {
		t.Fatal(err)
	}
	wantMinimum := time.Unix(highWaterUnix, 0).UTC().Add(idempotency.MaxTTL + idempotency.MaxFutureSkew)
	if expiresAt.Before(wantMinimum) {
		t.Fatalf("legacy evidence expires at %s before admissibility window %s", expiresAt, wantMinimum)
	}
}
