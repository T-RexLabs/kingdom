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
// Pending receipts remain resource debt; recording does not close them.
func RecordTerminalTx(ctx context.Context, tx *sql.Tx, id string, lease Lease, terminal providerexecution.RequestTerminal, now time.Time) error {
	ref, err := terminal.Reference()
	if err != nil {
		return err
	}
	b, err := Get(ctx, tx, id)
	if err != nil {
		return err
	}
	if b.Lease != lease || ref.InvocationID != b.Owner.InvocationID {
		return fmt.Errorf("provider terminal does not match its binding and lease")
	}
	if b.TerminalReference != nil {
		if *b.TerminalReference != ref {
			return fmt.Errorf("conflicting provider terminal evidence")
		}
		return nil
	}
	if b.DispatchIssuedAt == "" || b.AdmissionClosedAt == "" || now.IsZero() {
		return fmt.Errorf("provider terminal requires issued dispatch and closed admission")
	}
	owner, err := loadOwner(ctx, tx, b.Owner)
	if err != nil {
		return err
	}
	if err := owner.requireActive(b, false, time.Time{}); err != nil {
		return err
	}
	if err := matchAdmissions(ctx, tx, b.ID, terminal.Tools); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE provider_requests SET terminal_sha256=?,terminal_size_bytes=? WHERE binding_id=? AND terminal_sha256 IS NULL`, ref.SHA256, ref.SizeBytes, b.ID)
	return err
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
			projected[tool.ID] = tool
		default:
			return fmt.Errorf("provider terminal contains an unowned tool")
		}
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
