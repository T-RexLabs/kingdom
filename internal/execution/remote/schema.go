package remote

import (
	"context"
	"database/sql"
)

// InstallSchemaTx installs the isolated ledger. Product activation must place
// this schema in the canonical database migration, not call it at dispatch time.
// No deletion API exists: allocation and dispatch tombstones must be retained.
func InstallSchemaTx(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
 CREATE TABLE IF NOT EXISTS remote_runtimes (
 allocation INTEGER PRIMARY KEY AUTOINCREMENT,
 creation_id TEXT NOT NULL UNIQUE,
 owner_kind TEXT NOT NULL CHECK(owner_kind IN ('run','planning')),
 invocation_id TEXT NOT NULL UNIQUE,
 binding_id TEXT NOT NULL UNIQUE,
 lease_owner TEXT NOT NULL,
 lease_version INTEGER NOT NULL CHECK(lease_version>0),
 worker_id TEXT NOT NULL,
 gateway_id TEXT NOT NULL,
 workspace_id TEXT NOT NULL,
 spec_digest TEXT NOT NULL,
 sandbox_id TEXT NOT NULL DEFAULT '',
 generation TEXT NOT NULL DEFAULT '',
 main_instance_id TEXT NOT NULL DEFAULT '',
 admission_closed INTEGER NOT NULL DEFAULT 0 CHECK(admission_closed IN (0,1)),
 CHECK((sandbox_id='' AND generation='' AND main_instance_id='') OR
       (sandbox_id<>'' AND generation<>'' AND main_instance_id<>''))
 );
 CREATE UNIQUE INDEX IF NOT EXISTS remote_runtime_sandbox ON remote_runtimes(gateway_id,workspace_id,sandbox_id) WHERE sandbox_id<>'';
 CREATE TABLE IF NOT EXISTS remote_operations (
 operation_id TEXT PRIMARY KEY,
 allocation INTEGER NOT NULL REFERENCES remote_runtimes(allocation),
 kind TEXT NOT NULL CHECK(kind IN ('create','execute','stop')),
 payload_digest TEXT NOT NULL,
 delivery TEXT NOT NULL DEFAULT 'prepared' CHECK(delivery IN ('prepared','delivery_uncertain','admitted')),
 admission_digest TEXT NOT NULL DEFAULT '',
 CHECK((delivery='admitted' AND admission_digest<>'') OR (delivery<>'admitted' AND admission_digest=''))
 );
 CREATE UNIQUE INDEX IF NOT EXISTS remote_runtime_stop ON remote_operations(allocation) WHERE kind='stop';
 CREATE TABLE IF NOT EXISTS remote_commands (
 command_id TEXT PRIMARY KEY REFERENCES remote_operations(operation_id),
 allocation INTEGER NOT NULL REFERENCES remote_runtimes(allocation),
 binding_id TEXT NOT NULL,
 claim_id TEXT NOT NULL UNIQUE,
 payload_digest TEXT NOT NULL,
 sequence INTEGER NOT NULL CHECK(sequence>0),
 UNIQUE(binding_id,sequence)
 );`)
	return err
}
