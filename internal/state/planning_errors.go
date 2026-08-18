package state

import (
	"errors"
)

var (
	ErrPlanningSessionNotFound      = errors.New("planning session not found")
	ErrPlanningMessageNotFound      = errors.New("planning message not found")
	ErrPlanningOperationNotFound    = errors.New("planning operation not found")
	ErrPlanningTurnOutOfRange       = errors.New("planning turn index out of range")
	ErrPlanningLeaseConflict        = errors.New("planning operation lease conflict")
	ErrPlanningUniqueViolation      = errors.New("planning unique constraint violation")
	ErrPlanningSnapshotNotInSession = errors.New("planning context snapshot does not belong to session")
	// ErrPlanningContextSnapshotUnavailable means a session's adopted context
	// snapshot pointer does not resolve. Sessions are created with their
	// snapshot in one transaction, so this is an invariant violation rather
	// than a state a caller can reach or retry out of.
	ErrPlanningContextSnapshotUnavailable = errors.New("planning session has no resolvable context snapshot")
)
