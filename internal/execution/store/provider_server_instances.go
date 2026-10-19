package store

import (
	"context"
	"database/sql"
	"fmt"

	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/sqlitex"
)

// OpenProviderServerInstances lists the shared servers not yet proven stopped.
func (s *Store) OpenProviderServerInstances(ctx context.Context) ([]providerrequest.ServerInstance, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store database is required")
	}
	return providerrequest.OpenServerInstances(ctx, s.db)
}

// CloseProviderServerInstance records that a shared server was proven stopped.
func (s *Store) CloseProviderServerInstance(ctx context.Context, id string, closure providerrequest.ServerClosure) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	_, err := sqlitex.Transaction(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		return providerrequest.CloseServerInstanceTx(ctx, tx, id, closure, s.currentTime())
	}, sqlitex.Options{})
	return err
}

// AbortOrphanedCommandClaims closes the command claims of stopped servers that
// never reached a guardian. It is only safe while nothing is launching commands,
// as during startup.
func (s *Store) AbortOrphanedCommandClaims(ctx context.Context) (int64, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("store database is required")
	}
	var aborted int64
	_, err := sqlitex.Transaction(ctx, s.db, func(ctx context.Context, tx *sql.Tx) (err error) {
		aborted, err = providerrequest.AbortOrphanedCommandClaimsTx(ctx, tx, s.currentTime())
		return err
	}, sqlitex.Options{})
	return aborted, err
}
