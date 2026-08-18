CREATE INDEX idx_idempotency_keys_expires_at
    ON idempotency_keys(expires_at);

CREATE TABLE idempotency_retention_clock (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    high_water_unix INTEGER NOT NULL CHECK (high_water_unix >= 0)
);

INSERT INTO idempotency_retention_clock (singleton, high_water_unix)
VALUES (1, CAST(strftime('%s', 'now') AS INTEGER));

-- Legacy keys remain inadmissible, but their deduplication evidence is retained
-- for one maximum protocol TTL in case an old opaque key matches the new grammar.
UPDATE idempotency_keys
SET expires_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '+7 days', '+5 minutes')
WHERE expires_at IS NULL;
