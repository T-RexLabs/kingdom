package orchestration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"
	"time"

	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/runtimefs"
	"kingdom/internal/securefs"
)

const providerReceiptInvocationPassLimit = 128

type providerCompletionReceiptWire struct {
	ProtocolVersion      string          `json:"protocol_version"`
	AttemptID            string          `json:"attempt_id"`
	InvocationID         string          `json:"invocation_id"`
	CompletionCapability string          `json:"completion_capability"`
	FinalText            string          `json:"final_text"`
	StructuredPayload    json.RawMessage `json:"structured_payload"`
}

func (o *orchestrationLoop) ingestProviderCompletionReceipts(ctx context.Context) error {
	_, err := o.ingestProviderCompletionReceiptPass(ctx)
	return err
}

func (o *orchestrationLoop) ingestProviderCompletionReceiptPass(ctx context.Context) (int, error) {
	if o == nil || o.executionStore == nil || o.fsStore == nil || o.schemas == nil {
		return 0, fmt.Errorf("provider completion receipt ingestion is not configured")
	}
	o.receiptIngestionMu.Lock()
	defer o.receiptIngestionMu.Unlock()

	candidates, err := o.executionStore.ListProviderReceiptIngestionCandidates(ctx, o.receiptIngestionCursor, providerReceiptInvocationPassLimit)
	if err != nil {
		return 0, err
	}
	if len(candidates) == 0 {
		o.receiptIngestionCursor = ""
		return 0, nil
	}
	var ingestionErr error
	for _, candidate := range candidates {
		if err := o.ingestProviderCompletionReceiptCandidate(ctx, candidate); err != nil {
			var infrastructureErr *providerReceiptInfrastructureError
			if !errors.As(err, &infrastructureErr) {
				ingestionErr = errors.Join(ingestionErr, fmt.Errorf("ingest provider completion receipt invocation %q: %w", candidate.ProviderInvocationID, err))
			}
		}
		o.receiptIngestionCursor = candidate.ProviderInvocationID
	}
	return len(candidates), ingestionErr
}

func (o *orchestrationLoop) ingestProviderCompletionReceiptInvocation(ctx context.Context, providerInvocationID string) error {
	if o == nil || o.executionStore == nil || o.fsStore == nil || o.schemas == nil {
		return fmt.Errorf("provider completion receipt ingestion is not configured")
	}
	providerInvocationID = strings.TrimSpace(providerInvocationID)
	if providerInvocationID == "" {
		return fmt.Errorf("provider invocation id is required")
	}
	o.receiptIngestionMu.Lock()
	defer o.receiptIngestionMu.Unlock()
	candidate, found, err := o.executionStore.GetProviderInvocation(ctx, providerInvocationID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("provider invocation %q not found", providerInvocationID)
	}
	if candidate.CompletionMode != executionstore.ProviderCompletionModeReceiptRequired {
		return nil
	}
	return o.ingestProviderCompletionReceiptCandidate(ctx, candidate)
}

func (o *orchestrationLoop) ingestProviderCompletionReceiptCandidate(ctx context.Context, candidate executionstore.ProviderInvocation) error {
	if err := o.recoverProviderTerminalHandoffFile(ctx, candidate); err != nil {
		return err
	}
	entries, listErr := o.fsStore.ListProviderCompletionEntries(candidate.ProviderInvocationID)
	if listErr != nil && errors.Is(listErr, fs.ErrNotExist) {
		if providerReceiptInvocationTerminal(candidate.InvocationState) {
			return o.markMissingTerminalProviderReceiptInboxCleaned(ctx, candidate)
		}
		if err := o.persistProviderReceiptInfrastructureDisposition(ctx, candidate.ProviderInvocationID, "receipt_inbox_unavailable", "completion receipt inbox is unavailable"); err != nil {
			return err
		}
		return &providerReceiptInfrastructureError{cause: listErr}
	}
	if listErr != nil && !errors.Is(listErr, securefs.ErrLimitExceeded) {
		if err := o.persistProviderReceiptInfrastructureDisposition(ctx, candidate.ProviderInvocationID, "receipt_inbox_traversal_failed", "completion receipt inbox could not be traversed safely"); err != nil {
			return err
		}
		return &providerReceiptInfrastructureError{cause: listErr}
	}
	if errors.Is(listErr, securefs.ErrLimitExceeded) {
		if _, err := o.ingestProviderCompletionReceiptRequest(ctx, executionstore.IngestProviderCompletionReceiptRequest{
			ProviderInvocationID: candidate.ProviderInvocationID,
			EntryName:            ".inbox-overflow",
			EntryType:            "entry_count_overflow",
			Disposition:          executionstore.ProviderCompletionReceiptDispositionInvalidated,
			ReasonCode:           "receipt_entry_count_overflow",
			ReasonText:           "completion receipt inbox contains too many entries",
			ObservedAt:           o.providerReceiptObservedAt(),
		}); err != nil {
			return err
		}
	}

	node, err := o.providerReceiptNodeDefinition(ctx, candidate.NodeRunID)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := o.ingestProviderCompletionReceiptEntry(ctx, candidate, node, entry); err != nil {
			return err
		}
	}
	if len(entries) == 0 && providerReceiptInvocationTerminal(candidate.InvocationState) {
		o.cleanupTerminalProviderReceiptInbox(ctx, candidate.ProviderInvocationID)
	}
	return nil
}

func (o *orchestrationLoop) ingestProviderCompletionReceiptEntry(ctx context.Context, candidate executionstore.ProviderInvocation, node nodeDefinition, entry runtimefs.ProviderCompletionEntry) error {
	req := executionstore.IngestProviderCompletionReceiptRequest{
		ProviderInvocationID: candidate.ProviderInvocationID,
		EntryName:            entry.Name,
		WireByteCount:        maxInt64(entry.SizeBytes, 0),
		EntryType:            string(entry.Type),
		Disposition:          executionstore.ProviderCompletionReceiptDispositionRejected,
		ObservedAt:           o.providerReceiptObservedAt(),
	}
	if entry.Type != securefs.EntryTypeRegular {
		req.ReasonCode = "receipt_entry_not_regular"
		req.ReasonText = "completion receipt inbox entry is not a regular file"
		if _, err := o.ingestProviderCompletionReceiptRequest(ctx, req); err != nil {
			return err
		}
		if entry.Type != securefs.EntryTypeDirectory {
			o.removeDispositionedProviderCompletionEntry(candidate.ProviderInvocationID, entry)
		}
		return nil
	}
	if !strings.HasSuffix(entry.Name, ".json") {
		req.ReasonCode = "receipt_entry_not_json"
		req.ReasonText = "completion receipt inbox entry does not have a .json extension"
		if _, err := o.ingestProviderCompletionReceiptRequest(ctx, req); err != nil {
			return err
		}
		o.removeDispositionedProviderCompletionEntry(candidate.ProviderInvocationID, entry)
		return nil
	}

	receipt, readErr := o.fsStore.ReadProviderCompletionReceipt(candidate.ProviderInvocationID, entry.Name)
	if readErr != nil {
		req.ReasonCode = "receipt_read_failed"
		req.ReasonText = "completion receipt could not be read safely"
		if errors.Is(readErr, securefs.ErrLimitExceeded) {
			req.ReasonCode = "receipt_wire_too_large"
			req.ReasonText = "completion receipt exceeds the wire size limit"
			if _, err := o.ingestProviderCompletionReceiptRequest(ctx, req); err != nil {
				return err
			}
			o.removeDispositionedProviderCompletionEntry(candidate.ProviderInvocationID, entry)
			return nil
		}
		req.Disposition = executionstore.ProviderCompletionReceiptDispositionError
		if _, err := o.ingestProviderCompletionReceiptRequest(ctx, req); err != nil {
			return err
		}
		return &providerReceiptInfrastructureError{cause: readErr}
	}
	req.WireByteCount = int64(len(receipt.Wire))
	wireHash := sha256.Sum256(receipt.Wire)
	req.WireSHA256 = hex.EncodeToString(wireHash[:])
	if err := o.schemas.ValidateJSONBytes("completion_receipt", receipt.Wire); err != nil {
		req.ReasonCode = "receipt_schema_validation_failed"
		req.ReasonText = "completion receipt does not match the required schema"
		if _, ingestErr := o.ingestProviderCompletionReceiptRequest(ctx, req); ingestErr != nil {
			return ingestErr
		}
		o.removeDispositionedProviderCompletionReceipt(candidate.ProviderInvocationID, entry.Name, receipt.Identity)
		return nil
	}

	var wire providerCompletionReceiptWire
	if err := json.Unmarshal(receipt.Wire, &wire); err != nil {
		return fmt.Errorf("decode schema-validated provider completion receipt: %w", err)
	}
	req.ProtocolVersion = strings.TrimSpace(wire.ProtocolVersion)
	req.ObservedAttemptID = strings.TrimSpace(wire.AttemptID)
	req.ObservedInvocationID = strings.TrimSpace(wire.InvocationID)
	req.CompletionCapability = strings.TrimSpace(wire.CompletionCapability)
	structuredPayloadJSON, validationErr := validateProviderCompletionReceiptForNode(node, wire.FinalText, string(wire.StructuredPayload))
	if validationErr != nil {
		req.ReasonCode = "receipt_node_contract_validation_failed"
		req.ReasonText = "completion receipt does not satisfy the node output contract"
	} else {
		req.ValidationSucceeded = true
		req.FinalText = strings.TrimSpace(wire.FinalText)
		req.StructuredPayloadJSON = structuredPayloadJSON
		req.Disposition = executionstore.ProviderCompletionReceiptDispositionAccepted
	}
	if _, err := o.ingestProviderCompletionReceiptRequest(ctx, req); err != nil {
		return err
	}
	o.removeDispositionedProviderCompletionReceipt(candidate.ProviderInvocationID, entry.Name, receipt.Identity)
	return nil
}

type providerReceiptInfrastructureError struct {
	cause error
}

func (e *providerReceiptInfrastructureError) Error() string {
	if e == nil || e.cause == nil {
		return "provider completion receipt infrastructure error"
	}
	return e.cause.Error()
}

func (e *providerReceiptInfrastructureError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (o *orchestrationLoop) persistProviderReceiptInfrastructureDisposition(ctx context.Context, providerInvocationID, reasonCode, reasonText string) error {
	_, err := o.ingestProviderCompletionReceiptRequest(ctx, executionstore.IngestProviderCompletionReceiptRequest{
		ProviderInvocationID: providerInvocationID,
		EntryName:            ".inbox-error",
		EntryType:            "inbox_error",
		Disposition:          executionstore.ProviderCompletionReceiptDispositionError,
		ReasonCode:           reasonCode,
		ReasonText:           reasonText,
		ObservedAt:           o.providerReceiptObservedAt(),
	})
	return err
}

func (o *orchestrationLoop) ingestProviderCompletionReceiptRequest(ctx context.Context, req executionstore.IngestProviderCompletionReceiptRequest) (executionstore.IngestProviderCompletionReceiptResult, error) {
	var result executionstore.IngestProviderCompletionReceiptResult
	err := o.retrySQLiteBusyWrite(ctx, func(writeCtx context.Context) error {
		var err error
		result, err = o.executionStore.IngestProviderCompletionReceipt(writeCtx, req)
		return err
	})
	return result, err
}

func (o *orchestrationLoop) hydrateAcceptedProviderCompletionReceipt(ctx context.Context, result *asyncExecutionResult) error {
	if result == nil || result.completionMode != executionstore.ProviderCompletionModeReceiptRequired {
		return nil
	}
	invocation, found, err := o.executionStore.GetProviderInvocation(ctx, result.providerInvocationID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("provider invocation %q not found", result.providerInvocationID)
	}
	if invocation.InvocationState == executionstore.ProviderInvocationStateInvalidHandshake {
		result.completionReceiptEvidenceID = ""
		result.completionStructuredPayloadJSON = ""
		if normalizeExecutionOutcome(result.outcome, result.err).State == "success" {
			result.outcome = opencode.ExecutionOutcome{
				State:          "failed",
				ReasonCode:     "provider_incomplete_turn",
				FailureText:    "provider completion receipt handshake is invalid",
				EvidenceSource: "stream",
			}
			result.err = errorFromExecutionOutcome(result.outcome)
		}
		return nil
	}
	evidence, found, err := o.executionStore.GetProviderCompletionReceiptEvidence(ctx, result.providerInvocationID)
	if err != nil || !found {
		return err
	}
	payload, err := executionstore.ParseProviderCompletionReceiptEvidencePayload(evidence.PayloadJSON)
	if err != nil {
		return fmt.Errorf("parse accepted provider completion receipt evidence %q: %w", evidence.EvidenceID, err)
	}
	result.completionReceiptEvidenceID = evidence.EvidenceID
	result.outputText = payload.FinalText
	result.completionStructuredPayloadJSON = payload.StructuredPayloadJSON
	return nil
}

func (o *orchestrationLoop) providerReceiptNodeDefinition(ctx context.Context, nodeRunID string) (nodeDefinition, error) {
	var node nodeDefinition
	err := o.db.QueryRowContext(ctx, `
		SELECT nr.node_id, nr.required_role, wn.node_class
		FROM node_runs nr
		JOIN workflow_runs wr ON wr.run_id = nr.run_id
		JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		WHERE nr.node_run_id = ?`, strings.TrimSpace(nodeRunID)).Scan(&node.NodeID, &node.RequiredRole, &node.NodeClass)
	if err != nil {
		return nodeDefinition{}, fmt.Errorf("load completion receipt node contract for node run %q: %w", nodeRunID, err)
	}
	return node, nil
}

func (o *orchestrationLoop) removeDispositionedProviderCompletionEntry(providerInvocationID string, entry runtimefs.ProviderCompletionEntry) {
	if err := o.fsStore.RemoveProviderCompletionEntryIfIdentity(providerInvocationID, entry); err != nil {
		slog.Warn("provider completion entry cleanup deferred", "provider_invocation_id", providerInvocationID, "entry_name", entry.Name, "error", err)
	}
}

func (o *orchestrationLoop) removeDispositionedProviderCompletionReceipt(providerInvocationID, entryName string, identity securefs.FileIdentity) {
	if err := o.fsStore.RemoveProviderCompletionReceipt(providerInvocationID, entryName, identity); err != nil {
		slog.Warn("provider completion receipt cleanup deferred", "provider_invocation_id", providerInvocationID, "entry_name", entryName, "error", err)
	}
}

func (o *orchestrationLoop) cleanupTerminalProviderReceiptInbox(ctx context.Context, providerInvocationID string) {
	if _, err := o.fsStore.RemoveProviderCompletionStagingEntries(providerInvocationID); err != nil && !errors.Is(err, fs.ErrNotExist) {
		slog.Warn("provider completion staging cleanup deferred", "provider_invocation_id", providerInvocationID, "error", err)
		return
	}
	if err := o.fsStore.RemoveProviderCompletionInbox(providerInvocationID); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			slog.Warn("provider completion inbox cleanup deferred", "provider_invocation_id", providerInvocationID, "error", err)
			return
		}
	}
	if _, err := o.executionStore.MarkProviderReceiptInboxCleaned(ctx, executionstore.MarkProviderReceiptInboxCleanedRequest{
		ProviderInvocationID: providerInvocationID,
		CleanedAt:            o.providerReceiptObservedAt(),
	}); err != nil {
		slog.Warn("provider completion inbox cleanup marker deferred", "provider_invocation_id", providerInvocationID, "error", err)
	}
}

func (o *orchestrationLoop) markMissingTerminalProviderReceiptInboxCleaned(ctx context.Context, candidate executionstore.ProviderInvocation) error {
	if providerReceiptInvocationTerminal(candidate.InvocationState) {
		o.cleanupTerminalProviderReceiptInbox(ctx, candidate.ProviderInvocationID)
	}
	return nil
}

func providerReceiptInvocationTerminal(state executionstore.ProviderInvocationState) bool {
	switch state {
	case executionstore.ProviderInvocationStateSettledSuccess,
		executionstore.ProviderInvocationStateSettledFailed,
		executionstore.ProviderInvocationStateSettledCancelled,
		executionstore.ProviderInvocationStateSuperseded:
		return true
	default:
		return false
	}
}

func (o *orchestrationLoop) providerReceiptObservedAt() string {
	now := time.Now
	if o != nil && o.now != nil {
		now = o.now
	}
	return now().UTC().Format(time.RFC3339Nano)
}

func maxInt64(value, minimum int64) int64 {
	if value < minimum {
		return minimum
	}
	return value
}
