package orchestration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/runtimefs"
)

const providerTerminalHandoffPassLimit = 128
const providerTerminalHandoffWireVersion = "v1"

type providerTerminalHandoffWire struct {
	Version              string                               `json:"version"`
	ProviderInvocationID string                               `json:"provider_invocation_id"`
	AttemptID            string                               `json:"attempt_id"`
	RunID                string                               `json:"run_id"`
	NodeRunID            string                               `json:"node_run_id"`
	ExpectedLeaseOwner   string                               `json:"expected_lease_owner"`
	ExpectedLeaseVersion int64                                `json:"expected_lease_version"`
	StreamObservation    opencode.InvocationStreamObservation `json:"stream_observation"`
	ExecutionPayloadJSON string                               `json:"execution_payload_json"`
	CreatedAt            string                               `json:"created_at"`
}

type providerTerminalHandoffStreamEnvelope struct {
	AttemptID            string                                `json:"attempt_id"`
	ProviderInvocationID string                                `json:"provider_invocation_id"`
	CompletionMode       executionstore.ProviderCompletionMode `json:"completion_mode"`
	LeaseOwner           string                                `json:"lease_owner"`
	LeaseVersion         int64                                 `json:"lease_version"`
	Observation          opencode.InvocationStreamObservation  `json:"observation"`
}

func (o *orchestrationLoop) persistProviderTerminalHandoffFile(ctx context.Context, result *asyncExecutionResult, payloadJSON string) (executionstore.PersistProviderTerminalHandoffResult, error) {
	if o == nil || o.fsStore == nil || o.executionStore == nil || result == nil {
		return executionstore.PersistProviderTerminalHandoffResult{}, fmt.Errorf("provider terminal handoff persistence is not configured")
	}
	file, found, err := o.fsStore.ReadProviderTerminalHandoff(result.providerInvocationID)
	if err != nil {
		return executionstore.PersistProviderTerminalHandoffResult{}, err
	}
	var wire providerTerminalHandoffWire
	if found {
		wire, err = decodeProviderTerminalHandoffWire(file.Wire)
		if err != nil {
			return executionstore.PersistProviderTerminalHandoffResult{}, err
		}
		if err := validateProviderTerminalHandoffWireForResult(wire, *result, payloadJSON); err != nil {
			return executionstore.PersistProviderTerminalHandoffResult{}, err
		}
	} else {
		wire = providerTerminalHandoffWire{
			Version:              providerTerminalHandoffWireVersion,
			ProviderInvocationID: result.providerInvocationID,
			AttemptID:            result.attemptID,
			RunID:                result.run.RunID,
			NodeRunID:            result.attempt.NodeRunID,
			ExpectedLeaseOwner:   result.expectedLeaseOwner,
			ExpectedLeaseVersion: result.expectedLeaseVersion,
			StreamObservation:    result.providerStreamObservation,
			ExecutionPayloadJSON: payloadJSON,
			CreatedAt:            o.now().UTC().Format(time.RFC3339Nano),
		}
		if err := validateProviderTerminalHandoffWireForResult(wire, *result, payloadJSON); err != nil {
			return executionstore.PersistProviderTerminalHandoffResult{}, err
		}
		raw, marshalErr := json.Marshal(wire)
		if marshalErr != nil {
			return executionstore.PersistProviderTerminalHandoffResult{}, fmt.Errorf("marshal provider terminal handoff wire: %w", marshalErr)
		}
		file = runtimefs.ProviderTerminalHandoffFile{Wire: raw}
	}
	// Exact replays must cross the same filesystem publication boundary before
	// SQLite can accept the handoff.
	identity, persistErr := o.fsStore.PersistProviderTerminalHandoff(result.providerInvocationID, file.Wire)
	if persistErr != nil {
		return executionstore.PersistProviderTerminalHandoffResult{}, persistErr
	}
	file.Identity = identity

	var persisted executionstore.PersistProviderTerminalHandoffResult
	err = o.retrySQLiteBusyWrite(ctx, func(writeCtx context.Context) error {
		var persistErr error
		persisted, persistErr = o.executionStore.PersistProviderTerminalHandoff(writeCtx, providerTerminalHandoffStoreRequest(wire))
		return persistErr
	})
	if err != nil {
		return executionstore.PersistProviderTerminalHandoffResult{}, err
	}
	if err := o.fsStore.RemoveProviderTerminalHandoff(wire.ProviderInvocationID, file.Identity); err != nil {
		slog.Warn("provider terminal handoff file cleanup deferred", "provider_invocation_id", wire.ProviderInvocationID, "error", err)
	}
	return persisted, nil
}

func (o *orchestrationLoop) recoverProviderTerminalHandoffFile(ctx context.Context, candidate executionstore.ProviderInvocation) error {
	file, found, readErr := o.fsStore.ReadProviderTerminalHandoff(candidate.ProviderInvocationID)
	if !found {
		return nil
	}
	if providerReceiptInvocationTerminal(candidate.InvocationState) {
		if err := o.fsStore.RemoveProviderTerminalHandoff(candidate.ProviderInvocationID, file.Identity); err != nil {
			slog.Warn("terminal provider handoff file cleanup deferred", "provider_invocation_id", candidate.ProviderInvocationID, "error", err)
		}
		return nil
	}
	if readErr != nil {
		return &providerReceiptInfrastructureError{cause: readErr}
	}
	wire, err := decodeProviderTerminalHandoffWire(file.Wire)
	if err != nil {
		return &providerReceiptInfrastructureError{cause: err}
	}
	if err := validateProviderTerminalHandoffWireForCandidate(wire, candidate); err != nil {
		return &providerReceiptInfrastructureError{cause: err}
	}
	// Startup replay must verify the exact handoff file before restoring its
	// SQLite projection as well.
	identity, persistErr := o.fsStore.PersistProviderTerminalHandoff(candidate.ProviderInvocationID, file.Wire)
	if persistErr != nil {
		return &providerReceiptInfrastructureError{cause: persistErr}
	}
	file.Identity = identity
	if err := o.retrySQLiteBusyWrite(ctx, func(writeCtx context.Context) error {
		_, persistErr := o.executionStore.PersistProviderTerminalHandoff(writeCtx, providerTerminalHandoffStoreRequest(wire))
		return persistErr
	}); err != nil {
		return &providerReceiptInfrastructureError{cause: err}
	}
	if err := o.fsStore.RemoveProviderTerminalHandoff(candidate.ProviderInvocationID, file.Identity); err != nil {
		slog.Warn("recovered provider terminal handoff file cleanup deferred", "provider_invocation_id", candidate.ProviderInvocationID, "error", err)
	}
	return nil
}

func decodeProviderTerminalHandoffWire(raw []byte) (providerTerminalHandoffWire, error) {
	if int64(len(raw)) > runtimefs.ProviderTerminalHandoffWireLimit {
		return providerTerminalHandoffWire{}, fmt.Errorf("provider terminal handoff wire exceeds %d bytes", runtimefs.ProviderTerminalHandoffWireLimit)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var wire providerTerminalHandoffWire
	if err := decoder.Decode(&wire); err != nil {
		return providerTerminalHandoffWire{}, fmt.Errorf("decode provider terminal handoff wire: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = fmt.Errorf("unexpected trailing JSON value")
		}
		return providerTerminalHandoffWire{}, fmt.Errorf("decode provider terminal handoff wire: %w", err)
	}
	return wire, nil
}

func validateProviderTerminalHandoffWireForCandidate(wire providerTerminalHandoffWire, candidate executionstore.ProviderInvocation) error {
	if wire.Version != providerTerminalHandoffWireVersion {
		return fmt.Errorf("provider terminal handoff wire version is unsupported")
	}
	if wire.ProviderInvocationID != candidate.ProviderInvocationID || wire.AttemptID != candidate.AttemptID || wire.RunID != candidate.RunID || wire.NodeRunID != candidate.NodeRunID {
		return fmt.Errorf("provider terminal handoff wire lineage does not match the selected invocation")
	}
	if wire.ExpectedLeaseOwner == "" || wire.ExpectedLeaseOwner != strings.TrimSpace(wire.ExpectedLeaseOwner) || wire.ExpectedLeaseVersion <= 0 {
		return fmt.Errorf("provider terminal handoff wire lease lineage is invalid")
	}
	if wire.StreamObservation.ProviderSessionID != strings.TrimSpace(wire.StreamObservation.ProviderSessionID) {
		return fmt.Errorf("provider terminal handoff wire provider session is not canonical")
	}
	if candidate.CompletionMode != executionstore.ProviderCompletionModeReceiptRequired {
		return fmt.Errorf("provider terminal handoff wire invocation is not receipt-required")
	}
	if _, err := time.Parse(time.RFC3339Nano, wire.CreatedAt); err != nil {
		return fmt.Errorf("provider terminal handoff wire created_at is invalid: %w", err)
	}
	result, err := decodeExecutionPayloadJSON(wire.ExecutionPayloadJSON, "provider terminal handoff file payload")
	if err != nil {
		return err
	}
	handoff := executionstore.ProviderTerminalHandoff{
		HandoffID:            executionstore.ProviderTerminalHandoffID(wire.ProviderInvocationID),
		ProviderInvocationID: wire.ProviderInvocationID,
		AttemptID:            wire.AttemptID,
		RunID:                wire.RunID,
		NodeRunID:            wire.NodeRunID,
		ExpectedLeaseOwner:   wire.ExpectedLeaseOwner,
		ExpectedLeaseVersion: wire.ExpectedLeaseVersion,
	}
	if err := validateProviderTerminalHandoffResult(handoff, result, wire.StreamObservation); err != nil {
		return err
	}
	return nil
}

func validateProviderTerminalHandoffWireForResult(wire providerTerminalHandoffWire, result asyncExecutionResult, payloadJSON string) error {
	candidate := executionstore.ProviderInvocation{
		ProviderInvocationID: result.providerInvocationID,
		AttemptID:            result.attemptID,
		RunID:                result.run.RunID,
		NodeRunID:            result.attempt.NodeRunID,
		CompletionMode:       result.completionMode,
	}
	if err := validateProviderTerminalHandoffWireForCandidate(wire, candidate); err != nil {
		return err
	}
	if wire.ExecutionPayloadJSON != payloadJSON {
		return fmt.Errorf("provider terminal handoff file replay conflicts with the execution payload")
	}
	return nil
}

func validateProviderTerminalHandoffResult(handoff executionstore.ProviderTerminalHandoff, result asyncExecutionResult, observation opencode.InvocationStreamObservation) error {
	if result.attemptID != handoff.AttemptID || result.run.RunID != handoff.RunID || result.attempt.NodeRunID != handoff.NodeRunID || result.providerInvocationID != handoff.ProviderInvocationID || result.expectedLeaseOwner != handoff.ExpectedLeaseOwner || result.expectedLeaseVersion != handoff.ExpectedLeaseVersion {
		return fmt.Errorf("terminal handoff payload lineage does not match durable handoff")
	}
	if result.attempt.ExecutionMode != executionstore.ExecutionModeProviderBacked || result.attempt.CompletionMode != executionstore.ProviderCompletionModeReceiptRequired || result.completionMode != executionstore.ProviderCompletionModeReceiptRequired {
		return fmt.Errorf("terminal handoff payload is not a receipt-required provider execution")
	}
	if strings.TrimSpace(result.completionReceiptEvidenceID) != "" || strings.TrimSpace(result.completionStructuredPayloadJSON) != "" {
		return fmt.Errorf("terminal handoff payload contains post-receipt evidence")
	}
	payloadObservation, err := json.Marshal(result.providerStreamObservation)
	if err != nil {
		return fmt.Errorf("encode terminal handoff payload stream observation: %w", err)
	}
	wireObservation, err := json.Marshal(observation)
	if err != nil {
		return fmt.Errorf("encode terminal handoff wire stream observation: %w", err)
	}
	if !bytes.Equal(payloadObservation, wireObservation) {
		return fmt.Errorf("terminal handoff payload stream observation does not match durable handoff")
	}
	if result.providerSessionID != "" && result.providerSessionID != observation.ProviderSessionID {
		return fmt.Errorf("terminal handoff payload provider session does not match durable handoff")
	}
	return nil
}

func providerTerminalHandoffStoreRequest(wire providerTerminalHandoffWire) executionstore.PersistProviderTerminalHandoffRequest {
	return executionstore.PersistProviderTerminalHandoffRequest{
		ProviderInvocationID: wire.ProviderInvocationID,
		AttemptID:            wire.AttemptID,
		RunID:                wire.RunID,
		NodeRunID:            wire.NodeRunID,
		ExpectedLeaseOwner:   wire.ExpectedLeaseOwner,
		ExpectedLeaseVersion: wire.ExpectedLeaseVersion,
		StreamObservation:    wire.StreamObservation,
		PayloadJSON:          wire.ExecutionPayloadJSON,
		CreatedAt:            wire.CreatedAt,
	}
}

func (o *orchestrationLoop) convergeProviderTerminalHandoffs(ctx context.Context) (int, error) {
	return o.convergeProviderTerminalHandoffsWithLimit(ctx, providerTerminalHandoffPassLimit)
}

func (o *orchestrationLoop) convergeProviderTerminalHandoffsWithLimit(ctx context.Context, limit int) (int, error) {
	if o == nil || o.executionStore == nil {
		return 0, fmt.Errorf("provider terminal handoff convergence is not configured")
	}
	o.terminalHandoffMu.Lock()
	defer o.terminalHandoffMu.Unlock()

	handoffs, err := o.executionStore.ListPendingProviderTerminalHandoffs(ctx, o.terminalHandoffCursor, limit)
	if err != nil {
		return 0, err
	}
	if len(handoffs) == 0 {
		o.terminalHandoffCursor = ""
		return 0, nil
	}
	visited := 0
	for _, handoff := range handoffs {
		if err := o.convergeProviderTerminalHandoff(ctx, handoff); err != nil {
			var infrastructureErr *providerReceiptInfrastructureError
			if errors.As(err, &infrastructureErr) && infrastructureErr.durablyDispositioned {
				o.terminalHandoffCursor = handoff.ProviderInvocationID
				visited++
				continue
			}
			return visited, fmt.Errorf("converge provider terminal handoff %q: %w", handoff.HandoffID, err)
		}
		o.terminalHandoffCursor = handoff.ProviderInvocationID
		visited++
	}
	return visited, nil
}

func (o *orchestrationLoop) convergeProviderTerminalHandoff(ctx context.Context, handoff executionstore.ProviderTerminalHandoff) error {
	result, err := decodeExecutionPayloadJSON(handoff.PayloadJSON, fmt.Sprintf("provider terminal handoff %q payload", handoff.HandoffID))
	if err != nil {
		return err
	}
	if err := validateProviderTerminalHandoffPayload(handoff, result); err != nil {
		return err
	}
	result.terminalHandoffID = strings.TrimSpace(handoff.HandoffID)

	if err := o.ingestProviderCompletionReceiptInvocation(ctx, handoff.ProviderInvocationID); err != nil {
		return err
	}
	if err := o.hydrateAcceptedProviderCompletionReceipt(ctx, &result); err != nil {
		return err
	}
	claimant := "terminal_handoff:" + handoff.HandoffID
	if !o.claimExecutionSettlement(result.attemptID, claimant) {
		return nil
	}
	defer o.releaseExecutionSettlement(result.attemptID, claimant)
	return o.commitExecutionCompletion(ctx, result)
}

func validateProviderTerminalHandoffPayload(handoff executionstore.ProviderTerminalHandoff, result asyncExecutionResult) error {
	if strings.TrimSpace(handoff.HandoffID) != executionstore.ProviderTerminalHandoffID(handoff.ProviderInvocationID) {
		return fmt.Errorf("terminal handoff identity does not match invocation lineage")
	}
	var envelope providerTerminalHandoffStreamEnvelope
	if err := json.Unmarshal([]byte(handoff.StreamObservationJSON), &envelope); err != nil {
		return fmt.Errorf("decode terminal handoff stream observation: %w", err)
	}
	if envelope.AttemptID != handoff.AttemptID ||
		envelope.ProviderInvocationID != handoff.ProviderInvocationID ||
		envelope.CompletionMode != executionstore.ProviderCompletionModeReceiptRequired ||
		envelope.LeaseOwner != handoff.ExpectedLeaseOwner ||
		envelope.LeaseVersion != handoff.ExpectedLeaseVersion {
		return fmt.Errorf("terminal handoff stream lineage does not match durable handoff")
	}
	return validateProviderTerminalHandoffResult(handoff, result, envelope.Observation)
}
