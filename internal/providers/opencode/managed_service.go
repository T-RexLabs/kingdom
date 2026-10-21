package opencode

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
)

var ErrManagedExecutionUnavailable = errors.New("managed provider dispatch requires verified remote authority and tool integration")

type ManagedServiceConfig struct {
	Worker      ManagedWorker
	Requests    *providerrequest.Authority
	CaptureRoot string
	StopLimit   time.Duration
}

// ManagedService owns one remote server per invocation. It has no local
// executor, host account discovery, or host workspace access.
type ManagedService struct {
	worker      ManagedWorker
	authority   *providerrequest.Authority
	captureRoot string
	stopLimit   time.Duration
	mu          sync.Mutex
	closed      bool
	entries     map[string]*managedEntry
	attempts    map[string]string
}
type managedEntry struct {
	invocation, attempt string
	cancel              context.CancelFunc
	ready               chan struct{}
	cleanup             chan struct{}
	handle              ManagedHandle
	session             *requestSession
	spool               string
}

func NewManagedService(cfg ManagedServiceConfig) (*ManagedService, error) {
	if nilManagedDependency(cfg.Worker) || cfg.Requests == nil || cfg.CaptureRoot == "" {
		return nil, errors.New("managed worker, request authority and capture directory are required")
	}
	if cfg.StopLimit < 0 {
		return nil, errors.New("managed stop limit must be positive")
	}
	if cfg.StopLimit == 0 {
		cfg.StopLimit = 30 * time.Second
	}
	if err := os.MkdirAll(cfg.CaptureRoot, 0700); err != nil {
		return nil, err
	}
	return &ManagedService{worker: cfg.Worker, authority: cfg.Requests, captureRoot: cfg.CaptureRoot, stopLimit: cfg.StopLimit, entries: map[string]*managedEntry{}, attempts: map[string]string{}}, nil
}
func (*ManagedService) ExecutionBackend() providerexecution.Backend {
	return providerexecution.BackendOpenShell
}

func (s *ManagedService) Prepare(ctx context.Context, req ExecuteRequest) (result Prepared, err error) {
	if err := requirePrepareRequest(req); err != nil {
		return result, err
	}
	if req.ResumeFrom != nil || req.ProviderSessionID != "" {
		return result, errors.New("managed conversation import is not available")
	}
	provider, model, ok := strings.Cut(req.Model, "/")
	if !ok || provider == "" || model == "" {
		return result, errors.New("model must name a provider and a model")
	}
	preparation, err := s.authority.Preparation(ctx, req.InvocationID)
	if err != nil {
		return result, err
	}
	if preparation.ResourceID != req.AttemptID {
		return result, errors.New("managed attempt differs from canonical owner")
	}
	snapshot, hosted, err := s.authority.ExecutionAccess(ctx, req.InvocationID)
	if err != nil {
		return result, err
	}
	if hosted && (snapshot.Selection.Model != req.Model || snapshot.Selection.ThinkingEffort != req.Variant) {
		return result, errors.New("managed model selection differs from invocation access snapshot")
	}
	work, cancel := context.WithCancel(ctx)
	entry := &managedEntry{invocation: req.InvocationID, attempt: req.AttemptID, cancel: cancel, ready: make(chan struct{}), cleanup: make(chan struct{}, 1)}
	s.mu.Lock()
	if s.closed || s.entries[req.InvocationID] != nil || s.attempts[req.AttemptID] != "" {
		s.mu.Unlock()
		cancel()
		return result, errors.New("managed service is closed or invocation already owns resources")
	}
	s.entries[req.InvocationID], s.attempts[req.AttemptID] = entry, req.InvocationID
	s.mu.Unlock()
	defer func() {
		cancel()
		close(entry.ready)
		if err != nil {
			cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), s.stopLimit)
			defer stop()
			err = errors.Join(err, s.closeEntry(cleanup, entry))
		}
	}()
	entry.handle, err = s.worker.Provision(work, ManagedProvision{InvocationID: req.InvocationID, Preparation: preparation})
	if err != nil {
		return result, err
	}
	if nilManagedDependency(entry.handle) {
		return result, errors.New("managed worker returned no handle")
	}
	endpoint := entry.handle.Endpoint()
	if !path.IsAbs(endpoint.Workspace) || path.Clean(endpoint.Workspace) != endpoint.Workspace {
		return result, errors.New("managed workspace is invalid")
	}
	if err = s.authority.ValidateRemotePreparation(work, req.InvocationID, preparation, endpoint.Runtime, endpoint.Server); err != nil {
		return result, err
	}
	client, err := newManagedClient(entry.handle.Transport())
	if err != nil {
		return result, err
	}
	if err = client.Health(work); err != nil {
		return result, err
	}
	entry.spool, err = os.MkdirTemp(s.captureRoot, "invocation-")
	if err != nil {
		return result, err
	}
	entry.session = &requestSession{client: client, instanceID: endpoint.Runtime.MainInstanceID}
	entry.session.capture, err = startCapture(work, client, entry.spool)
	if err != nil {
		return result, err
	}
	if err = awaitPlugin(work, client, endpoint.Workspace); err != nil {
		return result, err
	}
	if err = client.ValidateConfiguration(work, endpoint.Workspace); err != nil {
		return result, err
	}
	profile, err := newProfile(req.System, profileActions(req.Agent))
	if err != nil {
		return result, err
	}
	generation, err := registerProfile(work, client, endpoint.Workspace, endpoint.Runtime.MainInstanceID, profile)
	if err != nil {
		return result, err
	}
	request := &request{directory: endpoint.Workspace, profile: profile, model: ModelRef{ProviderID: provider, ID: model, Variant: req.Variant}}
	sessionID, _, err := prepareSession(work, entry.session, req, request)
	if err != nil {
		return result, err
	}
	identity := providerexecution.RequestIdentity{InstanceID: endpoint.Runtime.MainInstanceID, Generation: generation, Location: endpoint.Workspace, SessionID: sessionID, ProfileID: profile.id, RequestID: profile.requestID}
	if err = entry.handle.AuthorizeRequest(work, identity); err != nil {
		return result, err
	}
	if err = s.authority.ValidateRemotePreparation(work, req.InvocationID, preparation, endpoint.Runtime, endpoint.Server); err != nil {
		return result, err
	}
	if err = entry.session.capture.healthy(); err != nil {
		return result, err
	}
	if err = work.Err(); err != nil {
		return result, err
	}
	result = Prepared{Backend: providerexecution.BackendOpenShell, RemoteRuntime: endpoint.Runtime, ProviderSessionID: sessionID, Identity: identity}
	return result, nil
}

// Execute stays closed until canonical remote dispatch, native effects, tools,
// and terminal closure are integrated. Preparation cannot bypass this gate.
func (*ManagedService) Execute(context.Context, ExecuteRequest) (ExecuteResponse, error) {
	return ExecuteResponse{}, ErrManagedExecutionUnavailable
}

func (s *ManagedService) closeEntry(ctx context.Context, e *managedEntry) error {
	e.cancel()
	select {
	case <-e.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case e.cleanup <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-e.cleanup }()
	var failures []error
	if !nilManagedDependency(e.handle) {
		failures = append(failures, e.handle.Close(ctx))
	} else {
		failures = append(failures, s.worker.CloseInvocation(ctx, e.invocation))
	}
	if e.session != nil {
		if e.session.capture != nil {
			e.session.capture.cancel()
			select {
			case <-e.session.capture.done:
				e.session.capture.stop()
			case <-ctx.Done():
				failures = append(failures, ctx.Err())
			}
		}
		e.session.client.CloseIdleConnections()
	}
	if err := errors.Join(failures...); err != nil {
		return err
	}
	// Execute is disabled: this spool contains preparation events only. Future
	// execution must stage verified evidence before releasing any capture.
	if e.spool != "" {
		if err := os.RemoveAll(e.spool); err != nil {
			return err
		}
	}
	s.mu.Lock()
	if s.entries[e.invocation] == e {
		delete(s.entries, e.invocation)
		delete(s.attempts, e.attempt)
	}
	s.mu.Unlock()
	return nil
}
func (s *ManagedService) Discard(ctx context.Context, invocation string) error {
	s.mu.Lock()
	entry := s.entries[invocation]
	s.mu.Unlock()
	if entry == nil {
		return nil
	}
	return s.closeEntry(ctx, entry)
}
func (s *ManagedService) Cancel(ctx context.Context, attempt string) error {
	s.mu.Lock()
	entry := s.entries[s.attempts[attempt]]
	s.mu.Unlock()
	if entry == nil {
		return &AttemptNotFoundError{AttemptID: attempt}
	}
	return s.closeEntry(ctx, entry)
}
func (s *ManagedService) CloseContext(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	entries := make([]*managedEntry, 0, len(s.entries))
	for _, entry := range s.entries {
		entry.cancel()
		entries = append(entries, entry)
	}
	s.mu.Unlock()
	var failures []error
	for _, entry := range entries {
		if err := s.closeEntry(ctx, entry); err != nil {
			failures = append(failures, fmt.Errorf("close managed invocation: %w", err))
		}
	}
	return errors.Join(failures...)
}
func (s *ManagedService) ForceClose() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.stopLimit)
	defer cancel()
	return s.CloseContext(ctx)
}

var _ Backend = (*ManagedService)(nil)
