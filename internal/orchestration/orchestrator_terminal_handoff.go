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

	"kingdom/internal/core/providerexecution"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/runtimefs"
)

const providerTerminalHandoffPassLimit = 128
const providerTerminalHandoffWireVersion = "v2"

func (o *orchestrationLoop) lockProviderTerminalHandoffFile(providerInvocationID string) func() {
	if o == nil {
		return func() {}
	}
	return o.terminalHandoffFileLocks.Lock(providerInvocationID)
}

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

type providerTerminalHandoffInfrastructureError struct {
	cause error
}

func (o *orchestrationLoop) markProviderTerminalHandoffPending(ctx context.Context, result asyncExecutionResult) error {
	if o == nil || o.executionStore == nil {
		return fmt.Errorf("provider terminal recovery marker is not configured")
	}
	err := o.executionStore.MarkProviderTerminalHandoffPending(ctx, executionstore.MarkProviderTerminalHandoffPendingRequest{
		ProviderInvocationID: result.providerInvocationID,
		AttemptID:            result.attemptID,
		RunID:                result.run.RunID,
		NodeRunID:            result.attempt.NodeRunID,
		ExpectedLeaseOwner:   result.expectedLeaseOwner,
		ExpectedLeaseVersion: result.expectedLeaseVersion,
		ProcessQuiescent:     result.providerStreamObservation.ProcessQuiescent,
		MarkedAt:             o.now().UTC().Format(time.RFC3339Nano),
	})
	if err == nil {
		return nil
	}
	return err
}

func (o *orchestrationLoop) markProviderTerminalRecoveryRequired(ctx context.Context, result asyncExecutionResult) error {
	if o == nil || o.executionStore == nil {
		return fmt.Errorf("provider terminal recovery marker is not configured")
	}
	return o.executionStore.MarkProviderTerminalRecoveryRequired(ctx, executionstore.MarkProviderTerminalRecoveryRequiredRequest{
		ProviderInvocationID: result.providerInvocationID,
		AttemptID:            result.attemptID,
		RunID:                result.run.RunID,
		NodeRunID:            result.attempt.NodeRunID,
		ExpectedLeaseOwner:   result.expectedLeaseOwner,
		ExpectedLeaseVersion: result.expectedLeaseVersion,
		ProcessQuiescent:     result.providerStreamObservation.ProcessQuiescent,
		MarkedAt:             o.now().UTC().Format(time.RFC3339Nano),
	})
}

func providerTerminalInvocationState(state executionstore.ProviderInvocationState) bool {
	switch state {
	case executionstore.ProviderInvocationStateSettledSuccess,
		executionstore.ProviderInvocationStateSettledFailed,
		executionstore.ProviderInvocationStateSettledCancelled,
		executionstore.ProviderInvocationStateSuperseded,
		executionstore.ProviderInvocationStateInvalidated:
		return true
	default:
		return false
	}
}

func (e *providerTerminalHandoffInfrastructureError) Error() string {
	if e == nil || e.cause == nil {
		return "provider terminal handoff infrastructure failure"
	}
	return e.cause.Error()
}

func (e *providerTerminalHandoffInfrastructureError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (o *orchestrationLoop) persistProviderTerminalHandoffFile(ctx context.Context, result *asyncExecutionResult, payloadJSON, observedAt string) (executionstore.PersistProviderTerminalHandoffResult, error) {
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
			StreamObservation:    providerexecution.CompactObservation(result.providerStreamObservation),
			ExecutionPayloadJSON: payloadJSON,
			CreatedAt:            observedAt,
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
		request := providerTerminalHandoffStoreRequest(wire)
		request.CopyContent = o.fsStore.CopyProviderContent
		persisted, persistErr = o.executionStore.PersistProviderTerminalHandoff(writeCtx, request)
		return persistErr
	})
	if err != nil {
		return executionstore.PersistProviderTerminalHandoffResult{}, err
	}
	o.removeProviderTerminalHandoffFileLocked(ctx, wire.ProviderInvocationID, file)
	return persisted, nil
}

func (o *orchestrationLoop) resetProviderTerminalHandoffFileRecoveryCursor() {
	if o == nil {
		return
	}
	o.maintenance.scanMu.Lock()
	o.terminalHandoffFileCursor = ""
	o.maintenance.fileAwaiting = false
	o.execMu.Lock()
	o.maintenance.fileReplayFailed = false
	o.execMu.Unlock()
	o.maintenance.scanMu.Unlock()
}

func (o *orchestrationLoop) removeProviderTerminalHandoffFile(ctx context.Context, providerInvocationID string, file runtimefs.ProviderTerminalHandoffFile) {
	if o == nil || o.fsStore == nil || o.executionStore == nil {
		return
	}
	unlock := o.lockProviderTerminalHandoffFile(providerInvocationID)
	defer unlock()
	o.removeProviderTerminalHandoffFileLocked(ctx, providerInvocationID, file)
}

func (o *orchestrationLoop) removeProviderTerminalHandoffFileLocked(ctx context.Context, providerInvocationID string, file runtimefs.ProviderTerminalHandoffFile) {
	if o == nil || o.fsStore == nil || o.executionStore == nil {
		return
	}
	if err := o.removeProviderContentStaging(ctx, providerInvocationID, file); err != nil {
		markErr := o.markProviderTerminalHandoffFileCleanupPending(ctx, providerInvocationID)
		slog.Warn("provider content staging cleanup deferred", "provider_invocation_id", providerInvocationID, "error", errors.Join(err, markErr))
		return
	}
	if err := o.fsStore.RemoveProviderTerminalHandoff(providerInvocationID, file.Identity); err != nil {
		markErr := o.markProviderTerminalHandoffFileCleanupPending(ctx, providerInvocationID)
		slog.Warn("provider terminal handoff file cleanup deferred", "provider_invocation_id", providerInvocationID, "error", errors.Join(err, markErr))
		return
	}
	clearCtx, cancel := detachedTimeoutContext(ctx, 5*time.Second)
	clearErr := o.executionStore.ClearProviderTerminalHandoffFileCleanupPending(clearCtx, providerInvocationID, "", o.now().UTC().Format(time.RFC3339Nano))
	cancel()
	if clearErr != nil {
		slog.Warn("provider terminal handoff file cleanup marker clear deferred", "provider_invocation_id", providerInvocationID, "error", clearErr)
	}
}

func (o *orchestrationLoop) removeProviderContentStaging(ctx context.Context, invocationID string, file runtimefs.ProviderTerminalHandoffFile) error {
	wire, err := decodeProviderTerminalHandoffWire(file.Wire)
	if err != nil || wire.ProviderInvocationID != invocationID {
		return nil
	}
	for _, ref := range providerTerminalHandoffStoreRequest(wire).ContentReferences {
		if err := ref.Validate(invocationID); err != nil {
			return err
		}
		if err := o.fsStore.RemoveProviderContent(ref); err != nil {
			return err
		}
	}
	return o.fsStore.RemoveProviderInvocationContent(ctx, invocationID)
}

func (o *orchestrationLoop) markProviderTerminalHandoffFileCleanupPending(ctx context.Context, providerInvocationID string) error {
	if o == nil || o.executionStore == nil {
		return fmt.Errorf("provider terminal handoff cleanup store is not configured")
	}
	markCtx, cancel := detachedTimeoutContext(ctx, 5*time.Second)
	defer cancel()
	return o.executionStore.MarkProviderTerminalHandoffFileCleanupPending(markCtx, providerInvocationID, o.now().UTC().Format(time.RFC3339Nano))
}

func (o *orchestrationLoop) cleanupProviderTerminalHandoffFile(ctx context.Context, candidate executionstore.ProviderTerminalHandoffFileCleanupCandidate) error {
	unlock := o.lockProviderTerminalHandoffFile(candidate.ProviderInvocationID)
	defer unlock()
	file, found, err := o.fsStore.ReadProviderTerminalHandoff(candidate.ProviderInvocationID)
	if err != nil {
		return err
	}
	if found {
		if err := o.removeProviderContentStaging(ctx, candidate.ProviderInvocationID, file); err != nil {
			return err
		}
		if err := o.fsStore.RemoveProviderTerminalHandoff(candidate.ProviderInvocationID, file.Identity); err != nil {
			return err
		}
	} else if err := o.fsStore.RemoveProviderInvocationContent(ctx, candidate.ProviderInvocationID); err != nil {
		return err
	}
	return o.executionStore.ClearProviderTerminalHandoffFileCleanupPending(ctx, candidate.ProviderInvocationID, candidate.MarkedAt, o.now().UTC().Format(time.RFC3339Nano))
}

func (o *orchestrationLoop) recoverProviderTerminalHandoffFile(ctx context.Context, candidate executionstore.ProviderInvocation) error {
	unlock := o.lockProviderTerminalHandoffFile(candidate.ProviderInvocationID)
	defer unlock()

	current, found, err := o.executionStore.GetProviderInvocation(ctx, candidate.ProviderInvocationID)
	if err != nil {
		return &providerTerminalHandoffInfrastructureError{cause: fmt.Errorf("reload provider invocation before handoff replay: %w", err)}
	}
	if !found {
		return &providerTerminalHandoffInfrastructureError{cause: fmt.Errorf("provider invocation %q disappeared before handoff replay", candidate.ProviderInvocationID)}
	}
	_, handoffFound, err := o.executionStore.GetProviderTerminalHandoff(ctx, executionstore.ProviderTerminalHandoffID(candidate.ProviderInvocationID))
	if err != nil {
		return &providerTerminalHandoffInfrastructureError{cause: fmt.Errorf("reload provider terminal handoff before file replay: %w", err)}
	}
	file, fileFound, readErr := o.fsStore.ReadProviderTerminalHandoff(candidate.ProviderInvocationID)
	if readErr != nil {
		return &providerTerminalHandoffInfrastructureError{cause: readErr}
	}
	if providerTerminalInvocationState(current.InvocationState) || handoffFound {
		if fileFound {
			if err := o.markProviderTerminalHandoffFileCleanupPending(ctx, candidate.ProviderInvocationID); err != nil {
				return &providerTerminalHandoffInfrastructureError{cause: fmt.Errorf("re-arm terminal handoff file cleanup before removal: %w", err)}
			}
			o.removeProviderTerminalHandoffFileLocked(ctx, candidate.ProviderInvocationID, file)
		} else {
			if err := o.fsStore.RemoveProviderInvocationContent(ctx, candidate.ProviderInvocationID); err != nil {
				return &providerTerminalHandoffInfrastructureError{cause: err}
			}
			o.clearProviderTerminalHandoffFileCleanupPending(ctx, candidate.ProviderInvocationID, "")
		}
		return nil
	}
	if !fileFound {
		return nil
	}
	wire, err := decodeProviderTerminalHandoffWire(file.Wire)
	if err != nil {
		return &providerTerminalHandoffInfrastructureError{cause: err}
	}
	if err := validateProviderTerminalHandoffWireForCandidate(wire, current); err != nil {
		return &providerTerminalHandoffInfrastructureError{cause: err}
	}
	// Re-arm cleanup debt before publication. A concurrent settlement can make
	// this candidate stale, but cannot leave a recreated terminal file unmarked
	// if the process stops before exact-identity removal.
	if err := o.markProviderTerminalHandoffFileCleanupPending(ctx, candidate.ProviderInvocationID); err != nil {
		return &providerTerminalHandoffInfrastructureError{cause: fmt.Errorf("mark replayed handoff file cleanup pending: %w", err)}
	}
	identity, persistErr := o.fsStore.PersistProviderTerminalHandoff(candidate.ProviderInvocationID, file.Wire)
	if persistErr != nil {
		return &providerTerminalHandoffInfrastructureError{cause: persistErr}
	}
	file.Identity = identity
	if err := o.retrySQLiteBusyWrite(ctx, func(writeCtx context.Context) error {
		request := providerTerminalHandoffStoreRequest(wire)
		request.CopyContent = o.fsStore.CopyProviderContent
		_, persistErr := o.executionStore.PersistProviderTerminalHandoff(writeCtx, request)
		return persistErr
	}); err != nil {
		current, currentFound, readbackErr := o.executionStore.GetProviderInvocation(ctx, candidate.ProviderInvocationID)
		if readbackErr == nil && currentFound && providerTerminalInvocationState(current.InvocationState) {
			// A normal worker may have durably settled after this bounded file
			// candidate was selected. The terminal transaction is authoritative;
			// this file is only leftover cleanup debt.
			o.removeProviderTerminalHandoffFileLocked(ctx, candidate.ProviderInvocationID, file)
			return nil
		}
		_, persisted, handoffReadErr := o.executionStore.GetProviderTerminalHandoff(ctx, executionstore.ProviderTerminalHandoffID(candidate.ProviderInvocationID))
		if handoffReadErr == nil && persisted {
			o.removeProviderTerminalHandoffFileLocked(ctx, candidate.ProviderInvocationID, file)
			return nil
		}
		if readbackErr != nil || handoffReadErr != nil {
			err = errors.Join(err, readbackErr, handoffReadErr)
		}
		return &providerTerminalHandoffInfrastructureError{cause: err}
	}
	o.removeProviderTerminalHandoffFileLocked(ctx, candidate.ProviderInvocationID, file)
	return nil
}

func (o *orchestrationLoop) clearProviderTerminalHandoffFileCleanupPending(ctx context.Context, providerInvocationID, expectedMarkedAt string) {
	if o == nil || o.executionStore == nil {
		return
	}
	clearCtx, cancel := detachedTimeoutContext(ctx, 5*time.Second)
	defer cancel()
	if err := o.executionStore.ClearProviderTerminalHandoffFileCleanupPending(clearCtx, providerInvocationID, expectedMarkedAt, o.now().UTC().Format(time.RFC3339Nano)); err != nil {
		slog.Warn("provider terminal handoff cleanup marker clear deferred", "provider_invocation_id", providerInvocationID, "error", err)
	}
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
	if candidate.CompletionMode != executionstore.ProviderCompletionModeServerResponse {
		return fmt.Errorf("provider terminal handoff invocation is not server-response")
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
	if result.attempt.ExecutionMode != executionstore.ExecutionModeProviderBacked || result.attempt.CompletionMode != executionstore.ProviderCompletionModeServerResponse || result.completionMode != executionstore.ProviderCompletionModeServerResponse {
		return fmt.Errorf("terminal handoff payload is not a server-response provider execution")
	}
	payloadObservation, err := json.Marshal(providerexecution.CompactObservation(result.providerStreamObservation))
	if err != nil {
		return fmt.Errorf("encode terminal handoff payload stream observation: %w", err)
	}
	wireObservation, err := json.Marshal(providerexecution.CompactObservation(observation))
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
	var payload executionOutboxPayload
	_ = json.Unmarshal([]byte(wire.ExecutionPayloadJSON), &payload) // wire validation already decoded this
	refs := append([]providerexecution.ContentReference(nil), wire.StreamObservation.DiagnosticContent...)
	for _, ref := range []*providerexecution.ContentReference{wire.StreamObservation.CanonicalTerminalTextRef, payload.OutputTextRef, payload.ProviderDiagnosticsRef} {
		if ref != nil {
			refs = append(refs, *ref)
		}
	}
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
		ContentReferences:    refs,
	}
}

func (o *orchestrationLoop) convergeProviderTerminalHandoff(ctx context.Context, handoff executionstore.ProviderTerminalHandoff) error {
	result, err := decodeExecutionPayloadJSON(handoff.PayloadJSON, fmt.Sprintf("provider terminal handoff %q payload", handoff.HandoffID))
	if err != nil {
		return err
	}
	if err := validateProviderTerminalHandoffPayload(handoff, result); err != nil {
		return err
	}
	if err := o.hydrateProviderPayloadContent(ctx, handoff.PayloadJSON, &result); err != nil {
		return err
	}
	result.terminalHandoffID = strings.TrimSpace(handoff.HandoffID)

	claimant := "terminal_handoff:" + handoff.HandoffID
	if !o.claimExecutionSettlement(result.attemptID, claimant) {
		return nil
	}
	defer o.releaseExecutionSettlement(result.attemptID, claimant)
	if err := o.commitExecutionCompletion(ctx, result); err != nil {
		return err
	}
	file, found, err := o.fsStore.ReadProviderTerminalHandoff(handoff.ProviderInvocationID)
	if err != nil {
		markErr := o.markProviderTerminalHandoffFileCleanupPending(ctx, handoff.ProviderInvocationID)
		slog.Warn("settled provider terminal handoff file cleanup deferred", "provider_invocation_id", handoff.ProviderInvocationID, "error", errors.Join(err, markErr))
	} else if found {
		o.removeProviderTerminalHandoffFile(ctx, handoff.ProviderInvocationID, file)
	}
	return nil
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
		envelope.CompletionMode != executionstore.ProviderCompletionModeServerResponse ||
		envelope.LeaseOwner != handoff.ExpectedLeaseOwner ||
		envelope.LeaseVersion != handoff.ExpectedLeaseVersion {
		return fmt.Errorf("terminal handoff stream lineage does not match durable handoff")
	}
	return validateProviderTerminalHandoffResult(handoff, result, envelope.Observation)
}
