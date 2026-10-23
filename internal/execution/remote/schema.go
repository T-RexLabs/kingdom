package remote

import (
	"context"
	"database/sql"

	"kingdom/internal/dbmigrations"
)

// InstallSchemaTx installs the canonical remote ledger schema in an isolated
// database. Product databases use the normal migration runner. Repeated calls
// are safe; no separate copy of the schema is maintained here.
func InstallSchemaTx(ctx context.Context, tx *sql.Tx) error {
	for _, name := range []string{"0009_remote_runtime_reservations.up.sql", "0012_managed_execution_evidence.up.sql", "0016_remote_recovery_requests.up.sql", "0017_workspace_import_verification.up.sql", "0018_remote_provider_launches.up.sql", "0019_remote_runtime_disposal.up.sql", "0020_remote_workspace_downloads.up.sql", "0021_remote_typed_runtime_deletion.up.sql", "0022_remote_native_tools.up.sql", "0023_remote_worker_manifests.up.sql", "0024_remote_capability_closure.up.sql", "0025_remote_never_created.up.sql"} {
		// 0016 rebuilds tables to extend their constraints. Once its request
		// table exists, later evidence tables can reference those rebuilt rows.
		// Rebuilding them again would violate those retained foreign keys.
		if name == "0016_remote_recovery_requests.up.sql" || name == "0018_remote_provider_launches.up.sql" || name == "0022_remote_native_tools.up.sql" {
			marker := "remote_recovery_requests"
			if name == "0018_remote_provider_launches.up.sql" {
				marker = "remote_provider_launches"
			}
			if name == "0022_remote_native_tools.up.sql" {
				marker = "remote_native_tools"
			}
			var applied int
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, marker).Scan(&applied); err != nil {
				return err
			}
			if applied != 0 {
				continue
			}
		}
		raw, err := dbmigrations.Read(name)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, string(raw)); err != nil {
			return err
		}
	}
	// Keep the never-reused allocator above any retained historical sequence.
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO remote_allocations(allocation) SELECT seq FROM sqlite_sequence WHERE name='remote_runtimes' AND seq>0`)
	return err
}
