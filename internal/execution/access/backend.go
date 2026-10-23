package access

import (
	"context"
	"database/sql"
	"fmt"
	"kingdom/internal/core/providerexecution"
)

// PinSourceBackendTx is called in the source creation transaction. Retrying a
// source never substitutes the currently configured execution backend.
func PinSourceBackendTx(ctx context.Context, tx *sql.Tx, kind, id string, backend providerexecution.Backend) error {
	if backend != providerexecution.BackendLocal && backend != providerexecution.BackendOpenShell {
		return ErrInvalid
	}
	table, key, err := backendTable(kind)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO `+table+`(`+key+`,backend)VALUES(?,?)ON CONFLICT(`+key+`)DO NOTHING`, id, backend); err != nil {
		return err
	}
	saved, err := SourceBackend(ctx, tx, kind, id)
	if err != nil {
		return err
	}
	if saved != backend {
		return fmt.Errorf("source execution backend is immutable")
	}
	return nil
}
func SourceBackend(ctx context.Context, q Reader, kind, id string) (providerexecution.Backend, error) {
	table, key, err := backendTable(kind)
	if err != nil {
		return "", err
	}
	var backend providerexecution.Backend
	if err = q.QueryRowContext(ctx, `SELECT backend FROM `+table+` WHERE `+key+`=?`, id).Scan(&backend); err != nil {
		return "", err
	}
	if backend != providerexecution.BackendLocal && backend != providerexecution.BackendOpenShell {
		return "", ErrInvalid
	}
	return backend, nil
}
func backendTable(kind string) (string, string, error) {
	switch kind {
	case "run":
		return "execution_run_backends", "run_id", nil
	case "planning":
		return "execution_planning_backends", "operation_id", nil
	default:
		return "", "", ErrInvalid
	}
}
func ProjectIncarnation(ctx context.Context, q Reader) (string, error) {
	var id string
	err := q.QueryRowContext(ctx, `SELECT id FROM execution_project_identity WHERE slot=1`).Scan(&id)
	if err != nil {
		return "", err
	}
	if !validID(id) {
		return "", ErrInvalid
	}
	return id, nil
}
