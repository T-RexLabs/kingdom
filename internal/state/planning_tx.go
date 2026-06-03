package state

import (
	"database/sql"
	"errors"
	"fmt"
)

type txRollbacker interface {
	Rollback() error
}

// rollbackPlanningTx preserves the primary failure while surfacing any rollback
// failure that would otherwise be discarded on the transaction error path.
func rollbackPlanningTx(tx txRollbacker, action string, cause error) error {
	if tx == nil {
		return cause
	}

	rollbackErr := tx.Rollback()
	if rollbackErr == nil || errors.Is(rollbackErr, sql.ErrTxDone) {
		return cause
	}

	rollbackFailure := fmt.Errorf("%s rollback: %w", action, rollbackErr)
	if cause == nil {
		return rollbackFailure
	}
	return errors.Join(cause, rollbackFailure)
}
