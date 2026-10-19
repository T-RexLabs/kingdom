package opencode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
)

// Responder produces a fixture's raw answer to a request. It stands in for the
// model server, so tests control the answer while ownership, dispatch,
// terminal evidence and settlement stay real.
type Responder interface {
	Execute(context.Context, ExecuteRequest) (ExecuteResponse, error)
}

// CancelResponder is implemented by responders that observe cancellation.
type CancelResponder interface {
	Cancel(context.Context, CancelRequest) error
}

// FixtureProvider runs a Responder behind the real request-ownership layer: it
// issues dispatch, closes admission, and records a verified terminal exactly as
// Service does, but with no server, plugin or captured events.
type FixtureProvider struct {
	authority *providerrequest.Authority
	responder Responder

	mu       sync.Mutex
	prepared map[string]Prepared
}

func NewFixtureProvider(authority *providerrequest.Authority, responder Responder) *FixtureProvider {
	return &FixtureProvider{authority: authority, responder: responder, prepared: map[string]Prepared{}}
}

// NewMemoryAdapter is the fixture provider with canned answers.
func NewMemoryAdapter(authority *providerrequest.Authority) *FixtureProvider {
	return NewFixtureProvider(authority, memoryResponder{})
}

type memoryResponder struct{}

func (memoryResponder) Execute(_ context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	// A string-based fixture may materialize the replay source for its canned
	// output builder.
	if req.OpenPrompt != nil {
		reader, err := req.OpenPrompt(context.Background())
		if err != nil {
			return ExecuteResponse{}, err
		}
		raw, readErr := io.ReadAll(reader)
		if err := errors.Join(readErr, reader.Close()); err != nil {
			return ExecuteResponse{}, err
		}
		req.Prompt = string(raw)
	}
	summary, err := buildMemoryOutput(req)
	if err != nil {
		return ExecuteResponse{}, err
	}
	return ExecuteResponse{OutputText: summary, Outcome: ExecutionOutcome{State: "success", ReasonCode: "execution_completed"}}, nil
}

func (p *FixtureProvider) Prepare(_ context.Context, req ExecuteRequest) (Prepared, error) {
	if strings.TrimSpace(req.InvocationID) == "" || strings.TrimSpace(req.AttemptID) == "" {
		return Prepared{}, fmt.Errorf("invocation and attempt identity are required")
	}
	location := filepath.Clean(req.WorkspacePath)
	if resolved, err := filepath.EvalSymlinks(location); err == nil {
		location = resolved
	}
	if !filepath.IsAbs(location) {
		return Prepared{}, fmt.Errorf("workspace path must be absolute")
	}
	token := strings.TrimSpace(req.InvocationID)
	prepared := Prepared{
		ProviderSessionID: "ses_fixture_" + token,
		Identity: providerexecution.RequestIdentity{
			InstanceID: "fixture", Generation: "fixture", Location: location,
			SessionID: "ses_fixture_" + token, ProfileID: "kd_fixture_" + token, RequestID: "msg_fixture_" + token,
		},
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.prepared[token]; exists {
		return Prepared{}, fmt.Errorf("provider invocation %s is already prepared", token)
	}
	p.prepared[token] = prepared
	return prepared, nil
}

func (p *FixtureProvider) Discard(_ context.Context, invocationID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.prepared, strings.TrimSpace(invocationID))
	return nil
}

func (p *FixtureProvider) Cancel(ctx context.Context, req CancelRequest) error {
	if responder, ok := p.responder.(CancelResponder); ok {
		return responder.Cancel(ctx, req)
	}
	return &AttemptNotFoundError{AttemptID: req.AttemptID}
}

func (p *FixtureProvider) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	if err := validateExecuteRequest(req); err != nil {
		return ExecuteResponse{}, err
	}
	token := strings.TrimSpace(req.InvocationID)
	p.mu.Lock()
	prepared, ok := p.prepared[token]
	delete(p.prepared, token)
	p.mu.Unlock()
	if !ok {
		return ExecuteResponse{}, fmt.Errorf("provider invocation %s was not prepared", token)
	}
	if err := p.authority.IssueDispatch(ctx, prepared.Identity); err != nil {
		return ExecuteResponse{}, err
	}
	if req.OnInvocationStarted != nil {
		req.OnInvocationStarted()
	}
	response, responseErr := p.responder.Execute(ctx, req)
	settle, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if err := p.authority.CloseAdmission(settle, prepared.Identity); err != nil {
		return response, errors.Join(responseErr, err)
	}
	terminal, staged, release, err := fixtureTerminal(req, response, responseErr)
	if err != nil {
		return response, errors.Join(responseErr, err)
	}
	if err := p.authority.RecordTerminal(settle, prepared.Identity, terminal); err != nil {
		release()
		return response, errors.Join(responseErr, err)
	}
	observation := response.StreamObservation
	observation.Request = &providerexecution.RequestObservation{Identity: prepared.Identity, Terminal: terminal}
	observation.ProviderSessionID = prepared.ProviderSessionID
	observation.FinalTurnID, observation.RawFinishReason, observation.NormalizedFinishReason = terminal.AssistantMessageID, terminal.RawFinishReason, terminal.FinishReason
	observation.CanonicalTerminalText, observation.CanonicalTerminalTextRef = "", terminal.Text
	observation.StagedTerminalContent, observation.TerminalTextOpener = nil, nil
	observation.StagedDiagnosticContent = staged.diagnostics
	observation.DiagnosticContent = staged.diagnosticRefs()
	observation.ToolCalls = nil
	if terminal.Text != nil {
		observation.StagedTerminalContent, observation.TerminalTextOpener = &staged.text, staged.text.Open
	}
	observation.ReleaseContent = func() error { release(); return nil }
	response.StreamObservation = observation
	response.ProviderSessionID = prepared.ProviderSessionID
	response.ProviderInvocationID = token
	return response, responseErr
}

type fixtureContent struct {
	text        providerexecution.StagedContent
	diagnostics []providerexecution.StagedContent
}

func (c fixtureContent) diagnosticRefs() []providerexecution.ContentReference {
	refs := make([]providerexecution.ContentReference, 0, len(c.diagnostics))
	for _, content := range c.diagnostics {
		refs = append(refs, content.Reference)
	}
	return refs
}

// fixtureTerminal describes a responder's answer as the verified terminal the
// real adapter would have produced, staging the content that backs it.
func fixtureTerminal(req ExecuteRequest, response ExecuteResponse, responseErr error) (providerexecution.RequestTerminal, fixtureContent, func(), error) {
	invocationID := strings.TrimSpace(req.InvocationID)
	observation := response.StreamObservation
	text := strings.TrimSpace(response.OutputText)
	if text == "" {
		text = strings.TrimSpace(observation.CanonicalTerminalText)
	}
	outcome := providerexecution.RequestOutcomeSucceeded
	switch {
	case errors.Is(responseErr, context.Canceled) || response.Outcome.State == "cancelled" || observation.Cancelled:
		outcome = providerexecution.RequestOutcomeInterrupted
	case responseErr != nil || observation.TimedOut || observation.ProviderErrorPresent || (response.Outcome.State != "" && response.Outcome.State != "success") || text == "":
		outcome = providerexecution.RequestOutcomeFailed
	}
	var staged []*stagedWriter
	release := func() {
		for _, writer := range staged {
			writer.discard()
		}
	}
	stage := func(pattern, content string) (providerexecution.StagedContent, error) {
		writer, err := newStagedWriter(pattern)
		if err != nil {
			return providerexecution.StagedContent{}, err
		}
		staged = append(staged, writer)
		if _, err := writer.Write([]byte(content)); err != nil {
			return providerexecution.StagedContent{}, err
		}
		return writer.finish(invocationID)
	}
	var content fixtureContent
	events, err := stage("kingdom-fixture-events-*", "[]")
	if err != nil {
		release()
		return providerexecution.RequestTerminal{}, content, nil, err
	}
	messages, err := stage("kingdom-fixture-messages-*", "[]")
	if err != nil {
		release()
		return providerexecution.RequestTerminal{}, content, nil, err
	}
	content.diagnostics = []providerexecution.StagedContent{events, messages}
	terminal := providerexecution.RequestTerminal{Outcome: outcome, Events: events.Reference, Messages: messages.Reference}
	if text != "" {
		if content.text, err = stage("kingdom-fixture-text-*", text); err != nil {
			release()
			return providerexecution.RequestTerminal{}, content, nil, err
		}
		terminal.Text = &content.text.Reference
		if outcome == providerexecution.RequestOutcomeSucceeded {
			terminal.AssistantMessageID, terminal.RawFinishReason, terminal.FinishReason = "msg_fixture_assistant_"+invocationID, "stop", "stop"
		}
	}
	return terminal, content, release, nil
}

func validateExecuteRequest(req ExecuteRequest) error {
	if req.AttemptID == "" || req.InvocationID == "" {
		return fmt.Errorf("attempt and invocation identity are required")
	}
	sources := 0
	if req.Prompt != "" {
		sources++
	}
	if req.OpenPrompt != nil {
		sources++
	}
	if req.WritePrompt != nil {
		sources++
	}
	if sources != 1 {
		return fmt.Errorf("exactly one prompt source is required")
	}
	return nil
}
