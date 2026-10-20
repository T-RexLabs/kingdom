package orchestration

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/state"
)

const recoveryBlockedEventType = "attempt.recovery_blocked"

// recoveryBlockedText tells the operator what keeps a step from being settled.
// Kingdom never guesses what an unfinished file change or command did.
func recoveryBlockedText(reason providerrequest.OpenReason) string {
	switch reason {
	case providerrequest.OpenNativeReceipts:
		return "A file change was in flight and the provider server making it is not yet proven stopped, so Kingdom cannot yet tell what it changed."
	case providerrequest.OpenCommands:
		return "A shell command was still running or its end is unconfirmed, so Kingdom cannot yet tell what it did."
	case providerrequest.OpenAdmissionOpen:
		return "The provider request is still active."
	default:
		return "Kingdom cannot yet prove what the provider request changed."
	}
}

// recordRecoveryBlocked tells the run timeline, once per attempt, that recovery
// is waiting on unresolved provider work.
func (o *orchestrationLoop) recordRecoveryBlocked(ctx context.Context, blocked []state.BlockedAttempt) {
	for _, item := range blocked {
		if err := o.emitRecoveryBlockedIfNeeded(ctx, item); err != nil {
			slog.Warn("failed to record blocked recovery", "run_id", item.RunID, "node_id", item.NodeID, "error", err)
		}
	}
}

func (o *orchestrationLoop) emitRecoveryBlockedIfNeeded(ctx context.Context, item state.BlockedAttempt) error {
	attemptID := composeAttemptID(item.RunID, item.NodeID, item.Attempt)
	var count int
	if err := o.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM events WHERE attempt_id = ? AND event_type = ?`, attemptID, recoveryBlockedEventType).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	text := recoveryBlockedText(item.Reason)
	payloadRaw, err := marshalExecutionAttemptEventPayload(attemptID, item.NodeID, item.NodeRunID, item.Attempt, "recovery_blocked", map[string]any{
		"blocked_reason": string(item.Reason),
		"operator_text":  text,
	})
	if err != nil {
		return fmt.Errorf("marshal recovery blocked payload: %w", err)
	}
	now := o.now().UTC().Format(time.RFC3339Nano)
	o.eventSeqMu.Lock()
	defer o.eventSeqMu.Unlock()
	_, err = o.events.Ingest(ctx, state.AttemptEventIngestionRequest{
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_recovery_blocked", sanitizeFileToken(item.RunID), sanitizeFileToken(item.NodeID), item.Attempt),
		RunID:           item.RunID,
		NodeRunID:       sql.NullString{String: item.NodeRunID, Valid: strings.TrimSpace(item.NodeRunID) != ""},
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       recoveryBlockedEventType,
		Seq:             o.nextEventSeq(ctx, attemptID),
		PayloadJSON:     string(payloadRaw),
		ActorID:         "runtime_orchestrator",
		RoleID:          item.RequiredRole,
		AuthoritySource: "runtime.recovery",
		ReasonCode:      "recovery_blocked",
		ReasonText:      text,
		CorrelationID:   "corr_orch_" + item.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_recovery_blocked", item.RunID, item.NodeRunID),
		CreatedAt:       now,
	})
	return err
}
