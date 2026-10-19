package providerrequest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"kingdom/internal/core/providerexecution"
)

// RecordTerminalTx stores the verified terminal record as the request's
// immutable evidence. Admission must already be closed, so no further tool can
// be admitted, and every durable admission must match a projected tool exactly.
// Pending receipts remain resource debt; recording does not close them. Settlement
// records the terminal in its own transaction, so a request has terminal
// evidence only once its outcome is canonical, or once it was abandoned.
func RecordTerminalTx(ctx context.Context, tx *sql.Tx, id string, lease Lease, terminal providerexecution.RequestTerminal, now time.Time) error {
	if now.IsZero() {
		return fmt.Errorf("provider terminal requires a record time")
	}
	b, ref, recorded, err := checkTerminal(ctx, tx, id, lease, terminal)
	if err != nil || recorded {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE provider_requests SET terminal_sha256=?,terminal_size_bytes=? WHERE binding_id=? AND terminal_sha256 IS NULL`, ref.SHA256, ref.SizeBytes, b.ID)
	return err
}

// CheckTerminalTx runs every check RecordTerminalTx does without recording, so
// a provider can refuse an inconsistent terminal before it hands one out.
func CheckTerminalTx(ctx context.Context, q Reader, id string, lease Lease, terminal providerexecution.RequestTerminal) error {
	_, _, _, err := checkTerminal(ctx, q, id, lease, terminal)
	return err
}

// checkTerminal reports whether the identical terminal is already recorded.
func checkTerminal(ctx context.Context, q Reader, id string, lease Lease, terminal providerexecution.RequestTerminal) (Binding, providerexecution.ContentReference, bool, error) {
	ref, err := terminal.Reference()
	if err != nil {
		return Binding{}, ref, false, err
	}
	b, err := Get(ctx, q, id)
	if err != nil {
		return b, ref, false, err
	}
	if b.Lease != lease || ref.InvocationID != b.Owner.InvocationID {
		return b, ref, false, fmt.Errorf("provider terminal does not match its binding and lease")
	}
	if b.TerminalReference != nil {
		if *b.TerminalReference != ref {
			return b, ref, false, fmt.Errorf("conflicting provider terminal evidence")
		}
		return b, ref, true, nil
	}
	if b.DispatchIssuedAt == "" || b.AdmissionClosedAt == "" {
		return b, ref, false, fmt.Errorf("provider terminal requires issued dispatch and closed admission")
	}
	owner, err := loadOwner(ctx, q, b.Owner)
	if err != nil {
		return b, ref, false, err
	}
	if err := owner.requireActive(b, false, time.Time{}); err != nil {
		return b, ref, false, err
	}
	// An abandoned record has no projected tools; its admissions are instead
	// proven closed by AbandonTx.
	if !terminal.Abandoned() {
		if err := matchAdmissions(ctx, q, b.ID, terminal.Tools); err != nil {
			return b, ref, false, err
		}
	}
	return b, ref, false, nil
}

type admittedCall struct{ id, messageID, tool, input string }

// matchAdmissions ties durable grants and command claims to the projected tool
// calls. A provider tool error may reflect refused admission; it never turns an
// absent grant into evidence for a completed call.
func matchAdmissions(ctx context.Context, q Reader, bindingID string, tools []providerexecution.RequestTool) error {
	projected := make(map[string]providerexecution.RequestTool, len(tools))
	for _, tool := range tools {
		switch tool.Name {
		case "write", "edit", "patch", "read", "glob", "grep", "shell":
		default:
			// Managed requests are refused every other tool, so only a refused
			// call can appear. A tool that completed was not admitted.
			if tool.Status != "error" {
				return fmt.Errorf("provider terminal contains an unowned tool")
			}
		}
		projected[tool.ID] = tool
	}
	admitted := make(map[string]bool, len(tools))
	for _, table := range []string{"provider_native_grants", "provider_command_calls"} {
		calls, err := admittedCalls(ctx, q, table, bindingID)
		if err != nil {
			return err
		}
		for _, call := range calls {
			tool, ok := projected[call.id]
			if !ok || tool.MessageID != call.messageID || tool.Name != call.tool {
				return fmt.Errorf("durable provider admission does not match the projected tool")
			}
			admittedInput, err := providerexecution.ToolInputDigests(json.RawMessage(call.input))
			if err != nil {
				return err
			}
			if !providerexecution.ToolInputsAgree(admittedInput, tool.Input) {
				return fmt.Errorf("durable provider admission input differs from the projected tool")
			}
			admitted[call.id] = true
		}
	}
	for id, tool := range projected {
		if !admitted[id] && tool.Status != "error" {
			return fmt.Errorf("completed provider tool has no durable admission")
		}
	}
	return nil
}

func admittedCalls(ctx context.Context, q Reader, table, bindingID string) ([]admittedCall, error) {
	rows, err := q.QueryContext(ctx, `SELECT call_id,message_id,tool,input_json FROM `+table+` WHERE binding_id=?`, bindingID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var calls []admittedCall
	for rows.Next() {
		var call admittedCall
		if err := rows.Scan(&call.id, &call.messageID, &call.tool, &call.input); err != nil {
			return nil, err
		}
		calls = append(calls, call)
	}
	return calls, rows.Err()
}

// AbandonTx closes a request that can no longer be proven, for example when its
// server is gone, and records the abandoned terminal. It succeeds only if every
// admitted tool and command is already closed by its exact receipt or guardian:
// no further effect is possible then, and the lost provider answer can only
// change what the work reported, never what it did. A pending admission keeps
// the request blocked, because its effect is unknown.
func AbandonTx(ctx context.Context, tx *sql.Tx, id string, lease Lease, now time.Time) error {
	b, err := Get(ctx, tx, id)
	if err != nil {
		return err
	}
	if b.Lease != lease || now.IsZero() {
		return fmt.Errorf("provider abandonment does not match its binding and lease")
	}
	if b.TerminalReference != nil {
		abandoned, err := isAbandoned(b)
		if err != nil || abandoned {
			return err
		}
		return fmt.Errorf("%w: request has provider terminal evidence and cannot be abandoned", ErrResourcesOpen)
	}
	stamp := now.UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `UPDATE provider_requests SET dispatch_issued_at=COALESCE(dispatch_issued_at,?),admission_closed_at=COALESCE(admission_closed_at,?) WHERE binding_id=?`, stamp, stamp, id); err != nil {
		return err
	}
	if err := RequireResourcesClosedTx(ctx, tx, id); err != nil {
		return err
	}
	return RecordTerminalTx(ctx, tx, id, lease, providerexecution.AbandonedRequestTerminal(b.Owner.InvocationID), now)
}

// AbandonOwnerTx abandons the request bound to an invocation.
func AbandonOwnerTx(ctx context.Context, tx *sql.Tx, owner Owner, now time.Time) (providerexecution.RequestIdentity, error) {
	b, err := RequiredForOwner(ctx, tx, owner)
	if err != nil {
		return providerexecution.RequestIdentity{}, err
	}
	return b.Identity, AbandonTx(ctx, tx, b.ID, b.Lease, now)
}
