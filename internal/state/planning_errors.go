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
)
