package planning

import (
	"context"
	"fmt"
	"sync"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/testutil/providerledger"
)

// preparer supplies the Prepare and Discard phases of a plannerExecutor to a
// fake that only implements Execute. It fixes a deterministic provider identity
// and records the order in which the agent drives the phases.
type preparer struct {
	mu          sync.Mutex
	calls       []string
	discarded   []string
	prepared    opencode.Prepared
	executing   string
	invocations int
	prepareErr  error
	activateErr error
}

func (p *preparer) Prepare(_ context.Context, req opencode.ExecuteRequest) (opencode.Prepared, error) {
	p.record("prepare")
	if p.prepareErr != nil {
		return opencode.Prepared{}, p.prepareErr
	}
	reusable := req.ProviderSessionID
	if req.ForceNewSession {
		reusable = ""
	}
	prepared := providerledger.Prepared(req.InvocationID, providerledger.SessionFor(req.InvocationID, reusable))
	p.mu.Lock()
	p.prepared, p.executing = prepared, req.InvocationID
	p.mu.Unlock()
	return prepared, nil
}

func (p *preparer) Discard(_ context.Context, invocationID string) error {
	p.record("discard")
	p.mu.Lock()
	p.discarded = append(p.discarded, invocationID)
	p.mu.Unlock()
	return nil
}

func (p *preparer) record(call string) {
	p.mu.Lock()
	p.calls = append(p.calls, call)
	p.mu.Unlock()
}

func (p *preparer) recorded() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

// invocation returns the canonical invocation context that the runner would
// hand to the agent. Activate marks it activated, as production does, once the
// canonical binding succeeds.
func (p *preparer) invocation(reusableProviderSessionID string) *ProviderInvocationContext {
	p.mu.Lock()
	p.invocations++
	id := fmt.Sprintf("invocation-%d", p.invocations)
	p.mu.Unlock()
	invocation := &ProviderInvocationContext{
		AttemptID:                 "attempt-" + id,
		LogicalSessionID:          "planner-session",
		InvocationID:              id,
		ReusableProviderSessionID: reusableProviderSessionID,
	}
	invocation.Activate = func(context.Context, opencode.Prepared) error {
		p.record("activate")
		if p.activateErr != nil {
			return p.activateErr
		}
		invocation.activated = true
		return nil
	}
	return invocation
}

// success is the response of a request the shared server completed with text.
func (p *preparer) success(text string) opencode.ExecuteResponse {
	p.mu.Lock()
	defer p.mu.Unlock()
	return providerledger.Success(p.prepared, p.executing, text)
}

func ledgerInvocation(invocation *ProviderInvocationContext) providerledger.Invocation {
	return providerledger.Invocation{ID: invocation.InvocationID, ReusableSessionID: invocation.ReusableProviderSessionID, Activate: invocation.Activate}
}

// completeProviderRequest plays the shared server for a fake agent: it
// prepares and activates the invocation's request, records the terminal
// evidence, and returns the metadata of a turn that settled with text. An agent
// called without a runner-issued invocation has nothing to bind.
func completeProviderRequest(ctx context.Context, invocation *ProviderInvocationContext, text string) (ProviderExecutionMetadata, error) {
	metadata := ProviderExecutionMetadata{FinalText: text, OutcomeState: "success", CompletionStructuredPayloadJSON: "{}"}
	if invocation == nil || invocation.Activate == nil {
		metadata.StreamObservation = terminalPlannerStreamObservation(text)
		return metadata, nil
	}
	response, err := providerledger.Complete(ctx, ledgerInvocation(invocation), text)
	if err != nil {
		return ProviderExecutionMetadata{}, err
	}
	metadata.ProviderSessionID, metadata.StreamObservation = response.ProviderSessionID, response.StreamObservation
	return metadata, nil
}

// failedProviderMetadata plays the shared server for a fake agent whose request
// ends without text. The request is prepared, activated and closed with a
// failed terminal, so what follows settles against a canonical invocation. The
// returned metadata is what the runner settles from.
func failedProviderMetadata(ctx context.Context, invocation *ProviderInvocationContext, metadata ProviderExecutionMetadata) (ProviderExecutionMetadata, error) {
	if invocation == nil || invocation.Activate == nil {
		return metadata, nil
	}
	observation, err := providerledger.Fail(ctx, ledgerInvocation(invocation))
	if err != nil {
		return metadata, err
	}
	observation.MeaningfulProgress = metadata.StreamObservation.MeaningfulProgress
	metadata.ProviderSessionID, metadata.StreamObservation = observation.ProviderSessionID, observation
	return metadata, nil
}

// failProviderRequest is the error of a fake agent whose request failed after
// the provider accepted it.
func failProviderRequest(ctx context.Context, invocation *ProviderInvocationContext, metadata ProviderExecutionMetadata, cause error) error {
	metadata, err := failedProviderMetadata(ctx, invocation, metadata)
	if err != nil {
		return err
	}
	return executionError(metadata, cause)
}

// terminalPlannerStreamObservation is the terminal observation of a turn whose
// request was never activated, for agents that settle without a runner-issued
// invocation.
func terminalPlannerStreamObservation(finalText string) providerexecution.InvocationStreamObservation {
	const invocationID = "invocation-unactivated"
	return providerledger.Success(providerledger.Prepared(invocationID, "ses-unactivated"), invocationID, finalText).StreamObservation
}
