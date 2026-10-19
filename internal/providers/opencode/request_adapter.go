package opencode

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"kingdom/internal/core/providerexecution"
)

// Prepared is the provider identity of a request that has been set up but not
// dispatched. Kingdom persists it with the canonical invocation before Execute
// is allowed to send any work.
type Prepared struct {
	ProviderSessionID string
	// Reused is false when a requested session could not be continued, so the
	// caller must submit its complete prompt rather than only the new input.
	Reused   bool
	Identity providerexecution.RequestIdentity
}

// request tracks one prepared request from Prepare to the end of Execute.
type request struct {
	invocationID string
	attemptID    string
	started      *serviceStart
	directory    string
	profile      profile
	identity     providerexecution.RequestIdentity
	reused       bool
	model        ModelRef

	mu        sync.Mutex
	executing bool
	stopped   bool
	stop      chan struct{}
	done      chan struct{}
}

// requestRegistry finds requests by the invocation that prepared them and by
// the attempt that a cancellation names.
type requestRegistry struct {
	mu           sync.Mutex
	byInvocation map[string]*request
	byAttempt    map[string]*request
}

func newRequestRegistry() *requestRegistry {
	return &requestRegistry{byInvocation: map[string]*request{}, byAttempt: map[string]*request{}}
}

func (r *requestRegistry) add(req *request) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byInvocation[req.invocationID]; exists {
		return fmt.Errorf("provider invocation %s is already prepared", req.invocationID)
	}
	if _, exists := r.byAttempt[req.attemptID]; exists {
		return fmt.Errorf("provider attempt %s already has a request in progress", req.attemptID)
	}
	r.byInvocation[req.invocationID], r.byAttempt[req.attemptID] = req, req
	return nil
}

func (r *requestRegistry) remove(req *request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.byInvocation, req.invocationID)
	delete(r.byAttempt, req.attemptID)
}

func (r *requestRegistry) idle() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byInvocation) == 0
}

func (r *requestRegistry) invocation(id string) *request {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.byInvocation[id]
}

func (r *requestRegistry) attempt(id string) *request {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.byAttempt[id]
}

// requestStop asks the request's Execute to stop once. It reports whether a
// stop was already pending.
func (r *request) requestStop() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return true
	}
	r.stopped = true
	close(r.stop)
	return false
}

// Prepare sets up the request's immutable profile and session without sending
// any work. The returned identity is what the canonical invocation binds.
func (s *Service) Prepare(ctx context.Context, req ExecuteRequest) (Prepared, error) {
	if s.requests == nil {
		return Prepared{}, fmt.Errorf("OpenCode managed execution is not configured")
	}
	if err := requirePrepareRequest(req); err != nil {
		return Prepared{}, err
	}
	started, err := s.ready(ctx)
	if err != nil {
		return Prepared{}, err
	}
	if err := started.capture.healthy(); err != nil {
		return Prepared{}, err
	}
	directory, err := filepath.EvalSymlinks(filepath.Clean(req.WorkspacePath))
	if err != nil {
		return Prepared{}, fmt.Errorf("resolve OpenCode workspace: %w", err)
	}
	providerID, modelID, ok := strings.Cut(req.Model, "/")
	if !ok || providerID == "" || modelID == "" {
		return Prepared{}, fmt.Errorf("model must name a provider and a model")
	}
	model := ModelRef{ProviderID: providerID, ID: modelID, Variant: req.Variant}
	p, err := newProfile(req.System, profileActions(req.Agent))
	if err != nil {
		return Prepared{}, err
	}
	// Each location loads its own plugin instance, so a location the server has
	// not served before must finish loading before its profile can be registered.
	if err := awaitPlugin(ctx, started.client, directory); err != nil {
		return Prepared{}, err
	}
	// A location can carry its own configuration, and a formatter that is on
	// there would change files outside Kingdom's tool admission.
	if err := started.client.ValidateConfiguration(ctx, directory); err != nil {
		return Prepared{}, err
	}
	generation, err := registerProfile(ctx, started.client, directory, started.instanceID, p)
	if err != nil {
		return Prepared{}, err
	}
	prepared := &request{
		invocationID: req.InvocationID, attemptID: req.AttemptID, started: started, directory: directory, profile: p, model: model,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	sessionID, reused, err := s.prepareSession(ctx, started, req, prepared)
	if err == nil {
		prepared.identity = providerexecution.RequestIdentity{InstanceID: started.instanceID, Generation: generation, Location: directory, SessionID: sessionID, ProfileID: p.id, RequestID: p.requestID}
		prepared.reused = reused
		err = s.requests.registry.add(prepared)
	}
	if err != nil {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.stopLimit)
		defer cancel()
		return Prepared{}, errors.Join(err, retireProfile(cleanup, started.client, directory, p.id))
	}
	return Prepared{ProviderSessionID: sessionID, Reused: reused, Identity: prepared.identity}, nil
}

func requirePrepareRequest(req ExecuteRequest) error {
	switch {
	case strings.TrimSpace(req.InvocationID) == "" || strings.TrimSpace(req.AttemptID) == "":
		return fmt.Errorf("invocation and attempt identity are required")
	case !filepath.IsAbs(req.WorkspacePath):
		return fmt.Errorf("workspace path must be absolute")
	case strings.TrimSpace(req.System) == "" || strings.TrimSpace(req.Agent) == "":
		return fmt.Errorf("agent and system instruction are required")
	}
	return nil
}

// prepareSession continues the requested session when this server has kept its
// history since creation, and otherwise creates a fresh one.
func (s *Service) prepareSession(ctx context.Context, started *serviceStart, req ExecuteRequest, prepared *request) (string, bool, error) {
	client, directory := started.client, prepared.directory
	if sessionID := req.ProviderSessionID; sessionID != "" && !req.ForceNewSession && started.capture.tracked(sessionID) {
		info, err := client.Session(ctx, directory, sessionID)
		if err != nil {
			return "", false, err
		}
		if filepath.Clean(info.Location.Directory) != directory {
			return "", false, fmt.Errorf("provider session belongs to a different workspace")
		}
		if err := requireEmptyInbox(ctx, client, directory, sessionID); err != nil {
			return "", false, err
		}
		if err := client.SwitchAgent(ctx, directory, sessionID, prepared.profile.id); err != nil {
			return "", false, err
		}
		if info.Model != prepared.model {
			if err := client.SwitchModel(ctx, directory, sessionID, prepared.model); err != nil {
				return "", false, err
			}
		}
		return sessionID, true, nil
	}
	sessionID, err := randomID("ses_")
	if err != nil {
		return "", false, err
	}
	// Tracking precedes creation so the session's origin event is retained.
	if err := started.capture.track(sessionID); err != nil {
		return "", false, err
	}
	spec := SessionSpec{ID: sessionID, Title: "Kingdom " + req.Agent, Agent: prepared.profile.id, Model: prepared.model}
	err = client.CreateSession(ctx, directory, spec)
	if err == nil {
		err = requireEmptyInbox(ctx, client, directory, sessionID)
	}
	if err != nil {
		started.capture.release(sessionID)
		return "", false, err
	}
	return sessionID, false, nil
}

func requireEmptyInbox(ctx context.Context, client *Client, directory, sessionID string) error {
	pending, err := client.PendingInbox(ctx, directory, sessionID)
	if err != nil {
		return err
	}
	if pending != 0 {
		return fmt.Errorf("provider session has undelivered input")
	}
	return nil
}

// Discard releases a prepared request that will not be executed.
func (s *Service) Discard(ctx context.Context, invocationID string) error {
	if s.requests == nil {
		return nil
	}
	prepared := s.requests.registry.invocation(invocationID)
	if prepared == nil {
		return nil
	}
	prepared.mu.Lock()
	executing := prepared.executing
	prepared.mu.Unlock()
	if executing {
		return fmt.Errorf("provider invocation %s is executing and must be cancelled", invocationID)
	}
	s.requests.registry.remove(prepared)
	return retireProfile(ctx, prepared.started.client, prepared.directory, prepared.profile.id)
}

// Cancel stops the attempt's request and waits for its Execute to settle the
// evidence. It interrupts only that request's session.
func (s *Service) Cancel(ctx context.Context, req CancelRequest) error {
	if s.requests == nil {
		return &AttemptNotFoundError{AttemptID: req.AttemptID}
	}
	prepared := s.requests.registry.attempt(req.AttemptID)
	if prepared == nil {
		return &AttemptNotFoundError{AttemptID: req.AttemptID}
	}
	prepared.requestStop()
	prepared.mu.Lock()
	executing := prepared.executing
	prepared.mu.Unlock()
	if !executing {
		return nil
	}
	select {
	case <-prepared.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
