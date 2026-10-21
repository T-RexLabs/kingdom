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
	for _, name := range []string{"0009_remote_runtime_reservations.up.sql", "0012_managed_execution_evidence.up.sql", "0016_remote_recovery_requests.up.sql"} {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, string(raw)); err != nil {
			return err
		}
	}
	return nil
}
