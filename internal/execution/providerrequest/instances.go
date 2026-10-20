package providerrequest

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ServerInstance is one shared server process that Kingdom started. Its
// ownership is the guardian's record of the exact process it supervises.
type ServerInstance struct {
	ID            string
	GuardID       string
	OwnershipJSON string
}

// ServerClosure says how an instance was proven to have stopped.
type ServerClosure string

const (
	// ClosedByGuardian is the guardian's own durable quiescence evidence.
	ClosedByGuardian ServerClosure = "guardian"
	// ClosedByAbsence is the proven absence of the exact process identity.
	ClosedByAbsence ServerClosure = "absence"
)

// RegisterServerInstanceTx records an instance before its guardian releases the
// server, so a crash can never leave a running server unrecorded.
func RegisterServerInstanceTx(ctx context.Context, tx *sql.Tx, instance ServerInstance, now time.Time) error {
	if strings.TrimSpace(instance.ID) == "" || strings.TrimSpace(instance.GuardID) == "" || strings.TrimSpace(instance.OwnershipJSON) == "" || now.IsZero() {
		return fmt.Errorf("server instance identity, guardian and ownership are required")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO provider_server_instances(instance_id,guard_id,ownership_json,started_at) VALUES(?,?,?,?)`,
		instance.ID, instance.GuardID, instance.OwnershipJSON, now.UTC().Format(time.RFC3339Nano))
	return err
}

// CloseServerInstanceTx records that an instance has stopped. Closing is
// idempotent, and the first proof is the one that stays.
func CloseServerInstanceTx(ctx context.Context, tx *sql.Tx, id string, closure ServerClosure, now time.Time) error {
	if closure != ClosedByGuardian && closure != ClosedByAbsence || now.IsZero() {
		return fmt.Errorf("server instance closure needs its basis and time")
	}
	result, err := tx.ExecContext(ctx, `UPDATE provider_server_instances SET quiescent_at=?,quiescence_basis=? WHERE instance_id=? AND quiescent_at IS NULL`,
		now.UTC().Format(time.RFC3339Nano), string(closure), id)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil || changed == 1 {
		return err
	}
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM provider_server_instances WHERE instance_id=?`, id).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return fmt.Errorf("server instance %q is not registered", id)
	}
	return nil
}

// OpenServerInstances lists the instances not yet proven to have stopped.
func OpenServerInstances(ctx context.Context, q Reader) ([]ServerInstance, error) {
	rows, err := q.QueryContext(ctx, `SELECT instance_id,guard_id,ownership_json FROM provider_server_instances WHERE quiescent_at IS NULL ORDER BY instance_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var instances []ServerInstance
	for rows.Next() {
		var instance ServerInstance
		if err := rows.Scan(&instance.ID, &instance.GuardID, &instance.OwnershipJSON); err != nil {
			return nil, err
		}
		instances = append(instances, instance)
	}
	return instances, rows.Err()
}

// AbortOrphanedCommandClaimsTx closes the command claims of stopped servers that
// never reached a guardian. A guardian acknowledges and persists its ownership
// before it releases a command, so a claim without one never ran, and
// RequireGuardianAdmissionTx refuses a guardian for an aborted claim, so a
// bridge still launching one can no longer run it.
func AbortOrphanedCommandClaimsTx(ctx context.Context, tx *sql.Tx, now time.Time) (int64, error) {
	if now.IsZero() {
		return 0, fmt.Errorf("command abort time is required")
	}
	result, err := tx.ExecContext(ctx, `UPDATE provider_command_calls SET aborted_at=?
		WHERE guard_id IS NULL AND aborted_at IS NULL AND completed_at IS NULL
		  AND binding_id IN (SELECT pr.binding_id FROM provider_requests pr JOIN provider_server_instances i ON i.instance_id=pr.instance_id WHERE i.quiescent_at IS NOT NULL)
		  AND NOT EXISTS (
		    SELECT 1 FROM provider_process_guards g
		    JOIN provider_requests pr ON pr.binding_id=provider_command_calls.binding_id
		    WHERE g.command_ordinal=provider_command_calls.command_ordinal
		      AND g.provider_invocation_id=COALESCE(pr.run_invocation_id,''))`, now.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
