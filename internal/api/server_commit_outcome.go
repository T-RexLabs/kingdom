package api

import (
	"context"
	"net/http"
	"time"

	"kingdom/internal/core/errorcatalog"
)

const commitOutcomeReadbackTimeout = time.Second

func commitOutcomeReadbackContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), commitOutcomeReadbackTimeout)
}

func writeCommitOutcomePending(w http.ResponseWriter, r *http.Request, authority, message string, details map[string]any) {
	if r != nil {
		markIdempotencyOutcomeIndeterminate(r.Context())
	}
	writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeIdempotencyPending, message, authority, details)
}
