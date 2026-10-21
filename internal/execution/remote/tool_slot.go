package remote

import (
	"context"
	"database/sql"
	"errors"
)

var ErrToolBusy = errors.New("remote tool slot remains occupied")

// RequireToolSlotTx derives exclusive tool ownership from retained operations.
// Only verified process closure or a no-start proof releases the slot. Time,
// transport errors, and missing identities never release it. Call in the same
// transaction as the fresh grant/claim and managed operation reservation.
func RequireToolSlotTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference, replayID string) error {
	if _, err := requireOwner(ctx, tx, owner, ref, true); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT o.operation_id FROM remote_operations o LEFT JOIN managed_executions m ON m.operation_id=o.operation_id LEFT JOIN remote_commands c ON c.command_id=o.operation_id LEFT JOIN remote_native_tools n ON n.operation_id=o.operation_id WHERE o.allocation=? AND o.operation_id<>? AND (m.purpose IN ('tool','native_tool') OR c.command_id IS NOT NULL OR n.operation_id IS NOT NULL) ORDER BY o.operation_id`, ref.Allocation, replayID)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if _, err := ReadProcessClosure(ctx, tx, owner, ref, id); err != nil {
			return ErrToolBusy
		}
	}
	return nil
}
