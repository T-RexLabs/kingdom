// Package planning implements decree-planning session orchestration.
//
// It layers planner-facing services on top of state.PlanningStore: creating and
// mutating sessions, running reply/regenerate/formalize operations, replaying
// and streaming planner events, recovering interrupted work, and mediating the
// OpenCode-backed planning agent contract.
package planning

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"kingdom/internal/state"
)

type Service struct {
	sessions       *sessionService
	operations     *operationService
	runner         *operationRunner
	recovery       *operationRecovery
	events         *operationEventService
	reconcileEvery time.Duration

	recoveryCtx    context.Context
	recoveryCancel context.CancelFunc
	recoveryDone   chan struct{}

	// Admission gate. closing flips true once Close has been called;
	// new writes after that get ErrServiceShuttingDown. activeWrites
	// counts admitted writes still in flight; drained closes the
	// instant activeWrites hits 0 after closing flipped, so Close
	// can wait on it with a deadline-bounded select. A raw
	// sync.RWMutex would have given us the same admission shape but
	// without the deadline awareness: RWMutex.Lock blocks
	// indefinitely on outstanding RLocks, which would trap
	// Service.Close past its supplied ctx if a slow planner-backed
	// write (GenerateTitle, RefreshContext) was mid-flight.
	writeMu      sync.Mutex
	closing      bool
	activeWrites int
	drained      chan struct{}
	forceOnce    sync.Once
	forced       chan struct{}
}

func NewService(store *state.PlanningStore, planner Agent, projectRoot string, nowRFC3339 func() string, idGenerator func(prefix string) string) *Service {
	clock := clockFunc(nowRFC3339)
	ids := idFunc(idGenerator)
	broker := newOperationEventBroker()
	liveBroker := newLiveOperationEventBroker()
	metrics := &plannerMetrics{}
	shared := runtimeDeps{
		store:       store,
		planner:     planner,
		now:         clock,
		ids:         ids,
		projectRoot: projectRoot,
		leaseOwner:  plannerLeaseOwner(),
		broker:      broker,
		liveBroker:  liveBroker,
		metrics:     metrics,
	}
	// One service-owned context drives both the recovery loop and every
	// runner-spawned goroutine. Close cancels this context, which fans
	// out: recovery loop exits, in-flight operations see Done at their
	// next chunk boundary and exit, and Start refuses any new operation
	// from racing in after Close.
	ctx, cancel := context.WithCancel(context.Background())
	runner := newOperationRunner(shared, ctx)
	svc := &Service{
		sessions:       newSessionService(shared, runner),
		operations:     newOperationService(shared, runner),
		runner:         runner,
		recovery:       newOperationRecovery(shared, runner),
		events:         newOperationEventService(shared),
		reconcileEvery: 30 * time.Second,
		recoveryCtx:    ctx,
		recoveryCancel: cancel,
		recoveryDone:   make(chan struct{}),
		forced:         make(chan struct{}),
	}
	svc.startRecoveryLoop()
	return svc
}

func (s *Service) startRecoveryLoop() {
	if s == nil || s.recovery == nil || s.reconcileEvery <= 0 {
		// Nothing to do — close the done channel so Close returns immediately.
		if s != nil && s.recoveryDone != nil {
			close(s.recoveryDone)
		}
		return
	}
	go func() {
		defer close(s.recoveryDone)
		ticker := time.NewTicker(s.reconcileEvery)
		defer ticker.Stop()
		for {
			select {
			case <-s.recoveryCtx.Done():
				return
			case <-ticker.C:
				_ = s.RecoverOperations(s.recoveryCtx)
			}
		}
	}()
}

// admitWrite registers an in-flight write at the Service facade. It
// rejects new writes once Close has flipped the closing flag, and
// returns ErrServiceShuttingDown so callers (and the API layer) can
// translate to 503. Pair with releaseWrite under defer.
//
// This sits at the Service layer so the gate covers the entire
// persist-then-Start sequence atomically: shutdown cannot land
// between a row being persisted and runner.Start being invoked,
// which would otherwise leak orphan queued/running rows.
func (s *Service) admitWrite() error {
	if s == nil {
		return ErrServiceShuttingDown
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closing {
		return ErrServiceShuttingDown
	}
	s.activeWrites++
	return nil
}

// releaseWrite decrements the in-flight counter. If Close is waiting
// (closing && activeWrites just hit 0), it closes the drained channel
// to wake the waiter. drained is set to nil so this can never
// double-close on a follow-up release.
func (s *Service) releaseWrite() {
	if s == nil {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.activeWrites--
	if s.closing && s.activeWrites == 0 && s.drained != nil {
		close(s.drained)
		s.drained = nil
	}
}

// beginClose flips the admission state and returns the channel callers
// should select on. If no writes are in flight, the returned channel
// is already closed so the caller's select short-circuits. Repeated
// calls return the same channel (or a closed one) so a defer that
// invokes Close after an explicit Close is a no-op.
func (s *Service) beginClose() <-chan struct{} {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closing {
		if s.drained == nil {
			closed := make(chan struct{})
			close(closed)
			return closed
		}
		return s.drained
	}
	s.closing = true
	if s.activeWrites == 0 {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	s.drained = make(chan struct{})
	return s.drained
}

// Close shuts down the service in three phases: stop new admissions, stop
// recovery, then drain in-flight runner operations. The supplied ctx bounds
// total shutdown time, but Close still attempts later phases and returns any
// joined timeout or drain errors.
//
// Tests and shutdown paths must call Close so runner goroutines and recovery
// do not outlive the service and keep mutating the DB after the process is
// supposed to be idle.
func (s *Service) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	drained := s.beginClose()
	forced := s.forced

	var writeDrainErr error
	select {
	case <-drained:
	case <-forced:
	case <-ctx.Done():
		writeDrainErr = fmt.Errorf("write drain: %w", ctx.Err())
	}

	if s.recoveryCancel != nil {
		s.recoveryCancel()
	}
	var recoveryDrainErr error
	if s.recoveryDone != nil {
		select {
		case <-s.recoveryDone:
		case <-forced:
		case <-ctx.Done():
			recoveryDrainErr = fmt.Errorf("recovery drain: %w", ctx.Err())
		}
	}

	var runnerDrainErr error
	if s.runner != nil {
		runnerDrainErr = s.runner.WaitWithAbort(ctx, forced)
	}

	return errors.Join(writeDrainErr, recoveryDrainErr, runnerDrainErr)
}

// ForceClose is the hard-stop counterpart to Close. It flips the
// admission gate and cancels the service-owned contexts immediately,
// but intentionally does not wait for in-flight writes, recovery, or
// runner goroutines to drain.
func (s *Service) ForceClose() {
	if s == nil {
		return
	}
	_ = s.beginClose()
	if s.recoveryCancel != nil {
		s.recoveryCancel()
	}
	s.forceOnce.Do(func() {
		close(s.forced)
	})
}

func (s *Service) RuntimeMetrics(ctx context.Context) (PlannerRuntimeMetrics, error) {
	if s == nil || s.operations == nil {
		return PlannerRuntimeMetrics{}, fmt.Errorf("planning service operation runtime is required")
	}
	return s.operations.RuntimeMetrics(ctx)
}

func (s *Service) ObserveOperationEventReplay(afterEventID string) {
	if s == nil || s.events == nil {
		return
	}
	s.events.observeReplay(afterEventID)
}

func (s *Service) CreateSession(ctx context.Context, title string, modelSelectionJSON string) (state.PlanningSession, error) {
	if s == nil || s.sessions == nil {
		return state.PlanningSession{}, fmt.Errorf("planning service session runtime is required")
	}
	if err := s.admitWrite(); err != nil {
		return state.PlanningSession{}, err
	}
	defer s.releaseWrite()
	return s.sessions.CreateSession(ctx, title, modelSelectionJSON)
}

func (s *Service) CreateOperation(ctx context.Context, planningSessionID string, input CreateOperationInput) (CreateOperationResult, error) {
	if s == nil || s.operations == nil {
		return CreateOperationResult{}, fmt.Errorf("planning service operation runtime is required")
	}
	if err := s.admitWrite(); err != nil {
		return CreateOperationResult{}, err
	}
	defer s.releaseWrite()
	return s.operations.CreateOperation(ctx, planningSessionID, input)
}

func (s *Service) GetOperation(ctx context.Context, operationID string) (state.PlanningOperation, bool, map[string]any, error) {
	if s == nil || s.operations == nil {
		return state.PlanningOperation{}, false, nil, fmt.Errorf("planning service operation runtime is required")
	}
	return s.operations.GetOperation(ctx, operationID)
}

func (s *Service) GetOperationByAcpRunID(ctx context.Context, acpRunID string) (state.PlanningOperation, bool, map[string]any, error) {
	if s == nil || s.operations == nil {
		return state.PlanningOperation{}, false, nil, fmt.Errorf("planning service operation runtime is required")
	}
	return s.operations.GetOperationByAcpRunID(ctx, acpRunID)
}

func (s *Service) CancelOperationByAcpRunID(ctx context.Context, acpRunID, reason string) (state.PlanningOperation, error) {
	if s == nil || s.operations == nil {
		return state.PlanningOperation{}, fmt.Errorf("planning service operation runtime is required")
	}
	if err := s.admitWrite(); err != nil {
		return state.PlanningOperation{}, err
	}
	defer s.releaseWrite()
	return s.operations.CancelOperationByAcpRunID(ctx, acpRunID, reason)
}

func (s *Service) ListOperationEventsAfterEventID(ctx context.Context, operationID string, afterEventID string) ([]state.PlanningOperationEvent, error) {
	if s == nil || s.events == nil {
		return nil, fmt.Errorf("planning service event runtime is required")
	}
	return s.events.ListOperationEventsAfterEventID(ctx, operationID, afterEventID)
}

func (s *Service) SubscribeOperationEvents(operationID string) (<-chan struct{}, func()) {
	if s == nil || s.events == nil {
		ch := make(chan struct{})
		close(ch)
		return ch, func() {}
	}
	return s.events.SubscribeOperationEvents(operationID)
}

func (s *Service) SubscribeLiveOperationEvents(operationID string) (<-chan LiveOperationEvent, func()) {
	if s == nil || s.events == nil {
		ch := make(chan LiveOperationEvent)
		close(ch)
		return ch, func() {}
	}
	return s.events.SubscribeLiveOperationEvents(operationID)
}

func (s *Service) RecoverOperations(ctx context.Context) error {
	if s == nil || s.recovery == nil {
		return fmt.Errorf("planning service recovery runtime is required")
	}
	return s.recovery.RecoverOperations(ctx, s.FinaliseIfStuck)
}

// FinaliseIfStuck transitions a running operation to failed if and only if
// its lease has been expired for at least StuckOperationAfter. Returns
// whether the operation was finalised by this call.
//
// This is the single shared progression primitive: both the recovery loop
// and the SSE handler's stale-check ticker call this. There is no per-
// observer counter — the decision is purely (now - lease_expires_at) >=
// threshold, so adding more observers cannot bias the failure trigger.
//
// Concurrent callers race on the operation row's lease fence inside
// FinalizeOperation; one wins, the other gets ErrPlanningLeaseConflict
// which we swallow (the op is terminal either way).
func (s *Service) FinaliseIfStuck(ctx context.Context, operationID string) (bool, error) {
	if s == nil || s.recovery == nil {
		return false, fmt.Errorf("planning service recovery runtime is required")
	}
	op, ok, err := s.operations.deps.store.GetOperation(ctx, operationID)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, ErrOperationNotFound
	}
	if recovered, err := s.recovery.convergeRecoveredRunningOperation(ctx, op); err != nil {
		return false, err
	} else if recovered {
		return true, nil
	}
	now := nowTime(s.operations.deps.nowString())
	if !IsStuckOperation(op, now) {
		return false, nil
	}
	if err := s.recovery.failStuck(ctx, op, idleFor(op, now)); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Service) UpdateSession(ctx context.Context, planningSessionID string, input UpdateSessionInput) (state.PlanningSession, error) {
	if s == nil || s.sessions == nil {
		return state.PlanningSession{}, fmt.Errorf("planning service session runtime is required")
	}
	if err := s.admitWrite(); err != nil {
		return state.PlanningSession{}, err
	}
	defer s.releaseWrite()
	return s.sessions.UpdateSession(ctx, planningSessionID, input)
}

func (s *Service) GenerateTitle(ctx context.Context, planningSessionID string) (state.PlanningSession, error) {
	if s == nil || s.sessions == nil {
		return state.PlanningSession{}, fmt.Errorf("planning service session runtime is required")
	}
	if err := s.admitWrite(); err != nil {
		return state.PlanningSession{}, err
	}
	defer s.releaseWrite()
	return s.sessions.GenerateTitle(ctx, planningSessionID)
}

func (s *Service) CreateGenerateTitleOperation(ctx context.Context, planningSessionID string) (state.PlanningSession, state.PlanningOperation, error) {
	if s == nil || s.sessions == nil {
		return state.PlanningSession{}, state.PlanningOperation{}, fmt.Errorf("planning service session runtime is required")
	}
	if err := s.admitWrite(); err != nil {
		return state.PlanningSession{}, state.PlanningOperation{}, err
	}
	defer s.releaseWrite()
	return s.sessions.CreateGenerateTitleOperation(ctx, planningSessionID)
}

func (s *Service) AutoTitleEligible(ctx context.Context, session state.PlanningSession) (bool, error) {
	if s == nil || s.sessions == nil {
		return false, fmt.Errorf("planning service session runtime is required")
	}
	return autoTitleEligibilityForSession(ctx, s.sessions.deps, session)
}

func (s *Service) RefreshContext(ctx context.Context, planningSessionID string) (state.PlanningContextSnapshot, state.PlanningSession, error) {
	if s == nil || s.sessions == nil {
		return state.PlanningContextSnapshot{}, state.PlanningSession{}, fmt.Errorf("planning service session runtime is required")
	}
	if err := s.admitWrite(); err != nil {
		return state.PlanningContextSnapshot{}, state.PlanningSession{}, err
	}
	defer s.releaseWrite()
	return s.sessions.RefreshContext(ctx, planningSessionID)
}

func (s *Service) ActivateVariant(ctx context.Context, planningSessionID, planningMessageID string) (state.PlanningMessage, error) {
	if s == nil || s.sessions == nil {
		return state.PlanningMessage{}, fmt.Errorf("planning service session runtime is required")
	}
	if err := s.admitWrite(); err != nil {
		return state.PlanningMessage{}, err
	}
	defer s.releaseWrite()
	return s.sessions.deps.store.SetActiveBranch(ctx, planningSessionID, planningMessageID)
}

// DeleteSession routes through the Service so admission gating is
// uniform across every write the API can dispatch. The DELETE
// endpoint previously hit the store directly, bypassing the gate —
// which let a delete land during shutdown after admission was
// closed for every other write. Returns (deleted, err) matching the
// store's contract: deleted=false means the row didn't exist.
func (s *Service) DeleteSession(ctx context.Context, planningSessionID string) (bool, error) {
	if s == nil || s.sessions == nil {
		return false, fmt.Errorf("planning service session runtime is required")
	}
	if err := s.admitWrite(); err != nil {
		return false, err
	}
	defer s.releaseWrite()
	return s.sessions.deps.store.DeleteSession(ctx, planningSessionID)
}

// GetMessageInSession is the session-scoped lookup the variant endpoints use
// to derive a message's turn/role before listing siblings; it returns
// state.ErrPlanningMessageNotFound when the (sessionID, messageID) pair does
// not match a stored row.
func (s *Service) GetMessageInSession(ctx context.Context, planningSessionID, planningMessageID string) (state.PlanningMessage, error) {
	if s == nil || s.sessions == nil {
		return state.PlanningMessage{}, fmt.Errorf("planning service session runtime is required")
	}
	return s.sessions.deps.store.GetMessageInSession(ctx, planningSessionID, planningMessageID)
}

// ListMessageBranches returns the branch alternatives at a message — every
// sibling sharing its parent — so the UI can offer switching between them.
func (s *Service) ListMessageBranches(ctx context.Context, planningSessionID, planningMessageID string) ([]state.PlanningMessage, error) {
	if s == nil || s.sessions == nil {
		return nil, fmt.Errorf("planning service session runtime is required")
	}
	return s.sessions.deps.store.SiblingsOf(ctx, planningSessionID, planningMessageID)
}
