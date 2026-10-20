package store

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/testutil/providerledger"
)

// boundSuccessObservation is the observation of a request that the shared
// server completed with text, the only form of provider evidence settlement
// accepts.
func boundSuccessObservation(invocationID, sessionID, text string) providerexecution.InvocationStreamObservation {
	return providerledger.Success(providerledger.Prepared(invocationID, sessionID), invocationID, text).StreamObservation
}

// recordBoundTerminal does what the shared server does around an activated
// request: it issues dispatch, closes admission, and stores the terminal
// evidence that canonical settlement requires.
func recordBoundTerminal(t testing.TB, s *Store, invocationID string, terminal providerexecution.RequestTerminal) {
	t.Helper()
	ctx := context.Background()
	binding, bound, err := providerrequest.ForOwner(ctx, s.db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: invocationID})
	if err != nil || !bound {
		t.Fatalf("load request binding for %s: bound=%t err=%v", invocationID, bound, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	now := s.currentTime()
	if binding.DispatchIssuedAt == "" {
		if err := providerrequest.IssueDispatchTx(ctx, tx, binding.ID, binding.Identity, binding.Lease, now); err != nil {
			t.Fatalf("issue dispatch for %s: %v", invocationID, err)
		}
	}
	if err := providerrequest.CloseAdmissionTx(ctx, tx, binding.ID, binding.Identity, binding.Lease, now); err != nil {
		t.Fatalf("close admission for %s: %v", invocationID, err)
	}
	if err := providerrequest.RecordTerminalTx(ctx, tx, binding.ID, binding.Lease, terminal); err != nil {
		t.Fatalf("record terminal for %s: %v", invocationID, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// claimBoundCommands dispatches the request if needed and admits managed
// commands up to the given ordinal, which command guardians can acknowledge.
func claimBoundCommands(t testing.TB, s *Store, invocationID string, upTo int64) {
	t.Helper()
	ctx := context.Background()
	binding, bound, err := providerrequest.ForOwner(ctx, s.db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: invocationID})
	if err != nil || !bound {
		t.Fatalf("load request binding for %s: bound=%t err=%v", invocationID, bound, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	now := s.currentTime()
	if binding.DispatchIssuedAt == "" {
		if err := providerrequest.IssueDispatchTx(ctx, tx, binding.ID, binding.Identity, binding.Lease, now); err != nil {
			t.Fatalf("issue dispatch for %s: %v", invocationID, err)
		}
	}
	for ordinal := int64(1); ordinal <= upTo; ordinal++ {
		claim, err := providerrequest.ClaimCommandTx(ctx, tx, providerrequest.NativeCall{
			BindingID: binding.ID, Identity: binding.Identity, MessageID: "message", CallID: fmt.Sprintf("call-%d", ordinal),
			Tool: "shell", Input: []byte(`{"command":"true"}`),
		}, now)
		if err != nil || claim.Ordinal != ordinal {
			t.Fatalf("claim command %d for %s: ordinal=%d err=%v", ordinal, invocationID, claim.Ordinal, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// completeBoundCommand records the quiescent guardian as the receipt of the
// claimed command, as the orchestrator does once the guardian is quiescent.
func completeBoundCommand(t testing.TB, s *Store, invocationID, guardID string, ordinal int64) {
	t.Helper()
	ctx := context.Background()
	var claimID, digest string
	if err := s.db.QueryRowContext(ctx, `SELECT c.claim_id, c.identity_digest FROM provider_command_calls c JOIN provider_requests r ON r.binding_id = c.binding_id WHERE r.run_invocation_id = ? AND c.command_ordinal = ?`, invocationID, ordinal).Scan(&claimID, &digest); err != nil {
		t.Fatalf("load command %d claim for %s: %v", ordinal, invocationID, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := providerrequest.CompleteCommandTx(ctx, tx, claimID, digest, guardID, s.currentTime()); err != nil {
		t.Fatalf("complete command %d for %s: %v", ordinal, invocationID, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// finishBoundRequest plays the shared server finishing the request of the
// settlement: the terminal projects every tool and command it admitted, and is
// stored with the dispatch and closed admission that settlement requires. The
// observation is updated to carry the stored terminal.
func finishBoundRequest(t testing.TB, s *Store, req *SettleProviderCompletionAndTerminalRequest) {
	t.Helper()
	ctx := context.Background()
	invocationID := req.Settlement.ProviderInvocationID
	rows, err := s.db.QueryContext(ctx, `SELECT call_id, message_id, tool, input_json FROM (
		SELECT call_id, message_id, tool, input_json, binding_id FROM provider_native_grants
		UNION ALL
		SELECT call_id, message_id, tool, input_json, binding_id FROM provider_command_calls
	) WHERE binding_id = ? ORDER BY call_id`, invocationID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	terminal := req.StreamObservation.Request.Terminal
	terminal.Tools = nil
	for rows.Next() {
		var tool providerexecution.RequestTool
		var input string
		if err := rows.Scan(&tool.ID, &tool.MessageID, &tool.Name, &input); err != nil {
			t.Fatal(err)
		}
		if tool.Input, err = providerexecution.ToolInputDigests(json.RawMessage(input)); err != nil {
			t.Fatal(err)
		}
		tool.Status = "completed"
		terminal.Tools = append(terminal.Tools, tool)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	req.StreamObservation.Request.Terminal = terminal
	req.StreamObservation.ToolCalls = terminal.ToolCalls()
	recordBoundTerminal(t, s, invocationID, terminal)
}

// withBoundRequest gives an activation the prepared session and request
// identity that every production activation carries.
func withBoundRequest(req ActivateProviderInvocationRequest) ActivateProviderInvocationRequest {
	if req.RequestIdentity != (providerexecution.RequestIdentity{}) {
		return req
	}
	req.ProviderSessionID = providerledger.SessionFor(req.ProviderInvocationID, req.ProviderSessionID)
	req.RequestIdentity = providerledger.Prepared(req.ProviderInvocationID, req.ProviderSessionID).Identity
	return req
}
