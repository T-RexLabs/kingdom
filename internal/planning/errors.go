package planning

import "errors"

var (
	ErrSessionNotFound            = errors.New("planning session not found")
	ErrMessageNotFound            = errors.New("planning message not found")
	ErrOperationNotFound          = errors.New("planning operation not found")
	ErrMessageNotInSession        = errors.New("planning message does not belong to session")
	ErrTurnIndexOutOfRange        = errors.New("planning turn index out of range")
	ErrLeaseConflict              = errors.New("planning operation lease conflict")
	ErrSessionInactive            = errors.New("planning session is not active")
	ErrOperationStuck             = errors.New("planning operation exceeded heartbeat deadline")
	ErrInteractiveOperationActive = errors.New("planning session already has an active interactive operation")
	ErrSessionBusy                = errors.New("planning session is busy")
	// ErrServiceShuttingDown is returned by every Service-facade write
	// once Close has flipped the admission gate. The API layer maps
	// this to HTTP 503 + Retry-After so the client can re-try after
	// the next process is up.
	ErrServiceShuttingDown = errors.New("planning service is shutting down")
)
