CREATE TABLE provider_contents (
    provider_invocation_id TEXT NOT NULL REFERENCES provider_invocations(provider_invocation_id),
    sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
    PRIMARY KEY(provider_invocation_id, sha256)
);
CREATE TABLE provider_content_chunks (
    provider_invocation_id TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    content BLOB NOT NULL CHECK(length(content) <= 65536),
    PRIMARY KEY(provider_invocation_id, sha256, ordinal),
    FOREIGN KEY(provider_invocation_id, sha256) REFERENCES provider_contents(provider_invocation_id, sha256)
);
CREATE TRIGGER provider_contents_immutable BEFORE UPDATE ON provider_contents
BEGIN SELECT RAISE(ABORT, 'provider content is immutable'); END;
CREATE TRIGGER provider_content_chunks_immutable BEFORE UPDATE ON provider_content_chunks
BEGIN SELECT RAISE(ABORT, 'provider content is immutable'); END;

-- A settled typed field becomes the sole canonical copy of matching handoff
-- content. The immutable reference remains valid after its transport chunks
-- are retired in the same settlement transaction.
CREATE TABLE provider_content_settlements (
    provider_invocation_id TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    settlement_id TEXT NOT NULL REFERENCES provider_completion_settlements(settlement_id),
    field TEXT NOT NULL CHECK(field IN ('final_text', 'provider_diagnostics_json')),
    PRIMARY KEY(provider_invocation_id, sha256),
    FOREIGN KEY(provider_invocation_id, sha256) REFERENCES provider_contents(provider_invocation_id, sha256)
);
CREATE TRIGGER provider_content_settlements_immutable BEFORE UPDATE ON provider_content_settlements
BEGIN SELECT RAISE(ABORT, 'provider content settlement is immutable'); END;
