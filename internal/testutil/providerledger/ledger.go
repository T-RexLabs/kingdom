// Package providerledger lets a test play the shared OpenCode server for a fake
// planner agent. A request that is bound to a canonical invocation settles only
// against the dispatch and terminal evidence the server records, so a fake that
// activates a request must record the same facts.
package providerledger

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/providers/opencode"
)

var ledgers sync.Map

// Register makes db findable by the invocations it holds. The returned function
// unregisters it.
func Register(db *sql.DB) func() {
	ledgers.Store(db, struct{}{})
	return func() { ledgers.Delete(db) }
}

// Prepared is a complete, deterministic provider request identity for one
// invocation bound to the given provider session.
func Prepared(invocationID, sessionID string) opencode.Prepared {
	token := token(invocationID)
	return opencode.Prepared{
		ProviderSessionID: sessionID,
		Identity: providerexecution.RequestIdentity{
			InstanceID: "instance-test", Generation: "generation-test", Location: "/opencode-test",
			SessionID: sessionID, ProfileID: "profile_" + token, RequestID: "request_" + token,
		},
	}
}

// SessionFor is the provider session a request of this invocation runs in: the
// reusable one when offered, otherwise a new session.
func SessionFor(invocationID, reusableSessionID string) string {
	if reusableSessionID != "" {
		return reusableSessionID
	}
	return "ses_" + token(invocationID)
}

func token(invocationID string) string {
	var out strings.Builder
	for _, r := range invocationID {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			out.WriteRune(r)
		} else {
			out.WriteByte('_')
		}
	}
	return out.String()
}

// SuccessTerminal is the terminal record of a request that completed with text.
func SuccessTerminal(invocationID string, text *providerexecution.ContentReference) providerexecution.RequestTerminal {
	return providerexecution.RequestTerminal{
		Outcome: providerexecution.RequestOutcomeSucceeded, AssistantMessageID: "assistant-message",
		RawFinishReason: "stop", FinishReason: "stop", Text: text,
		Events:   providerexecution.ReferenceText(invocationID, "events"),
		Messages: providerexecution.ReferenceText(invocationID, "messages"),
	}
}

// Success is the response of a request the server completed with text, in the
// form the planner requires before it settles a turn.
func Success(prepared opencode.Prepared, invocationID, text string) opencode.ExecuteResponse {
	text = strings.TrimSpace(text)
	ref := providerexecution.ReferenceText(invocationID, text)
	return opencode.ExecuteResponse{
		OutputText:        text,
		ProviderSessionID: prepared.ProviderSessionID,
		Outcome:           opencode.ExecutionOutcome{State: "success", ReasonCode: "execution_completed"},
		StreamObservation: opencode.InvocationStreamObservation{
			Request:                &providerexecution.RequestObservation{Identity: prepared.Identity, Terminal: SuccessTerminal(invocationID, &ref)},
			ProviderSessionID:      prepared.ProviderSessionID,
			FinalTurnID:            "assistant-message",
			RawFinishReason:        "stop",
			NormalizedFinishReason: "stop",
			CanonicalTerminalText:  text,
		},
	}
}

// TerminalObservation is the observation of a request the server ended without
// text: failed, or interrupted by cancellation. Its terminal evidence is stored.
func TerminalObservation(ctx context.Context, prepared opencode.Prepared, invocationID, outcome string) (opencode.InvocationStreamObservation, error) {
	terminal := providerexecution.RequestTerminal{
		Outcome: outcome,
		Events:  providerexecution.ReferenceText(invocationID, "events"), Messages: providerexecution.ReferenceText(invocationID, "messages"),
	}
	return opencode.InvocationStreamObservation{
		Request:           &providerexecution.RequestObservation{Identity: prepared.Identity, Terminal: terminal},
		ProviderSessionID: prepared.ProviderSessionID,
		Cancelled:         outcome == providerexecution.RequestOutcomeInterrupted,
	}, RecordTerminal(ctx, invocationID, terminal)
}

// Invocation is what a fake agent receives from the runner for one request.
type Invocation struct {
	ID                string
	ReusableSessionID string
	Activate          func(context.Context, opencode.Prepared) error
}

// Complete prepares and activates the invocation's request, records the
// server's terminal evidence, and returns the response of a turn that settled
// with text.
func Complete(ctx context.Context, invocation Invocation, text string) (opencode.ExecuteResponse, error) {
	prepared := Prepared(invocation.ID, SessionFor(invocation.ID, invocation.ReusableSessionID))
	prepared.Reused = invocation.ReusableSessionID != ""
	if err := invocation.Activate(ctx, prepared); err != nil {
		return opencode.ExecuteResponse{}, err
	}
	response := Success(prepared, invocation.ID, text)
	return response, RecordTerminal(ctx, invocation.ID, response.StreamObservation.Request.Terminal)
}

// Fail prepares and activates the invocation's request and closes it with a
// failed terminal, so the failure settles against a canonical invocation.
func Fail(ctx context.Context, invocation Invocation) (opencode.InvocationStreamObservation, error) {
	prepared := Prepared(invocation.ID, SessionFor(invocation.ID, invocation.ReusableSessionID))
	prepared.Reused = invocation.ReusableSessionID != ""
	if err := invocation.Activate(ctx, prepared); err != nil {
		return opencode.InvocationStreamObservation{}, err
	}
	return TerminalObservation(ctx, prepared, invocation.ID, providerexecution.RequestOutcomeFailed)
}

// RecordTerminal does what the server does around a request: it issues
// dispatch, closes admission, and stores the terminal evidence that canonical
// settlement requires. The invocation must already be activated.
func RecordTerminal(ctx context.Context, invocationID string, terminal providerexecution.RequestTerminal) error {
	ledger, err := ledgerOf(ctx, invocationID)
	if err != nil {
		return err
	}
	tx, err := ledger.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	binding, err := providerrequest.Get(ctx, tx, invocationID)
	if err != nil {
		return err
	}
	// The lease is judged at the moment the invocation was activated, the same
	// clock that issued it.
	var openedAt string
	if err := tx.QueryRowContext(ctx, `SELECT opened_at FROM planning_provider_invocations WHERE planning_provider_invocation_id=?`, invocationID).Scan(&openedAt); err != nil {
		return err
	}
	now, err := time.Parse(time.RFC3339Nano, openedAt)
	if err != nil {
		return err
	}
	if err := providerrequest.IssueDispatchTx(ctx, tx, invocationID, binding.Identity, binding.Lease, now); err != nil {
		return err
	}
	if err := providerrequest.CloseAdmissionTx(ctx, tx, invocationID, binding.Identity, binding.Lease, now); err != nil {
		return err
	}
	if err := providerrequest.RecordTerminalTx(ctx, tx, invocationID, binding.Lease, terminal, now); err != nil {
		return err
	}
	return tx.Commit()
}

// ledgerOf finds the registered database that holds an activated invocation's
// request. Single-connection databases that are busy are skipped and retried,
// because another test may hold theirs for as long as it likes.
func ledgerOf(ctx context.Context, invocationID string) (*sql.DB, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		var ledger *sql.DB
		ledgers.Range(func(key, _ any) bool {
			db := key.(*sql.DB)
			if db.Stats().InUse != 0 {
				return true
			}
			queryCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			var bound int
			if err := db.QueryRowContext(queryCtx, `SELECT count(*) FROM provider_requests WHERE binding_id=?`, invocationID).Scan(&bound); err == nil && bound == 1 {
				ledger = db
				return false
			}
			return true
		})
		if ledger != nil {
			return ledger, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, fmt.Errorf("no registered ledger holds provider request %q", invocationID)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
