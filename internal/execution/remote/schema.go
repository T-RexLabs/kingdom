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
	raw, err := dbmigrations.Read("0009_remote_runtime_reservations.up.sql")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, string(raw))
	return err
}
