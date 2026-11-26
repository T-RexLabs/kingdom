package planning

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"kingdom/internal/core/providerexecution"
	"log"
	"strings"
	"sync"
	"time"

	"kingdom/internal/acp"
	"kingdom/internal/sqlitex"
	"kingdom/internal/state"
)

// terminalCancelWriteTimeout bounds the persistence of a cancel-driven
// terminal row. The runner owns a separate, revocable handoff context for
// this write because its execution context is already cancelled. Close
// revokes it at the shutdown deadline, preventing late DB writes.
const terminalCancelWriteTimeout = 5 * time.Second

type planningOperationLeaseTicker interface {
	C() <-chan time.Time
	Stop()
}

type timePlanningOperationLeaseTicker struct {
	ticker *time.Ticker
}

func newTimePlanningOperationLeaseTicker(interval time.Duration) planningOperationLeaseTicker {
	return &timePlanningOperationLeaseTicker{ticker: time.NewTicker(interval)}
}

func (t *timePlanningOperationLeaseTicker) C() <-chan time.Time {
	return t.ticker.C
}

func (t *timePlanningOperationLeaseTicker) Stop() {
	t.ticker.Stop()
}

type operationStartSource string

const (
	operationStartSourceForeground operationStartSource = "foreground"
	operationStartSourceRecovery   operationStartSource = "recovery"
)

type operationRunner struct {
	deps   runtimeDeps
	events *operationEventService
	// ctx is the service-owned context — cancelled by Service.Close so
	// every in-flight execute() observes Done at the next chunk boundary
	// and exits, allowing Wait() to drain. Sharing it with the recovery
	// loop means a single recoveryCancel terminates both shutdown paths.
	ctx context.Context
	// handoffCtx remains live only long enough to record cancellation terminal
	// state after ctx is cancelled. Service.Close and ForceClose revoke it when
	// they can no longer wait for the runner, so it never escapes DB ownership.
	handoffCtx     context.Context
	cancelHandoffs context.CancelFunc

	mu            sync.Mutex
	running       map[string]struct{}
	cancels       map[string]context.CancelFunc
	cancelReasons map[string]string
	idle          chan struct{}
	// cancelDrainErr accumulates errors observed while the service ctx
	// was cancelled — most importantly, cancel-write timeouts from the
	// bounded writeCtx in cancelOperation. Surfaced via Wait →
	// Service.Close so kingdomd's exit code reflects the failure
	// instead of silently dropping the lost cancellation.
	//
	// The DB row may stay `running` if the cancel write never lands;
	// next-boot recovery catches it via the 3× TTL stuck-finalise
	// path (~120s worst-case lag). The shutdown contract demands an
	// honest exit code on the way out, not that the drain itself
	// always succeeded.
	//
	// Single accumulated error (not a slice cleared in Wait) so
	// repeated Wait/Close calls remain truthful.
	cancelDrainErr error
	// afterExecuteClaimed is a test seam for the narrow interval after the
	// canonical execution outcome is known and before its heartbeat is stopped.
	afterExecuteClaimed func()
	// wg tracks Start-launched goroutines so Service.Close can wait for
	// them. wg.Add is performed under mu, paired with the ctx-cancelled
	// guard below, so a goroutine either gets counted or is rejected
	// atomically — no race where Close cancels between the Add and the
	// goroutine launch.
	wg sync.WaitGroup
}

// sequenceCursor serialises monotonic sequence-number reservations for the
// per-operation event stream. It is shared between the main goroutine and the
// streaming agent's onChunk callback, both of which append events
// concurrently. Without this lock, two concurrent appends could observe the
// same `next` value and both insert events with identical `seq`, violating
// the UNIQUE(operation_id, seq) constraint.
type sequenceCursor struct {
	mu   sync.Mutex
	next int
}

func newSequenceCursor(start int) *sequenceCursor {
	return &sequenceCursor{next: start}
}

// Reserve atomically returns the current next value and advances the cursor.
func (c *sequenceCursor) Reserve() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	seq := c.next
	c.next++
	return seq
}

func newOperationRunner(deps runtimeDeps, ctx context.Context) *operationRunner {
	// The runner issues operation leases, so the same clock must judge them when
	// a provider request binds to one.
	deps.store.SetLeaseClock(func() time.Time { return nowTime(deps.nowString()) })
	idle := make(chan struct{})
	close(idle)
	handoffCtx, cancelHandoffs := context.WithCancel(context.Background())
	return &operationRunner{
		deps:           deps,
		events:         newOperationEventService(deps),
		ctx:            ctx,
		handoffCtx:     handoffCtx,
		cancelHandoffs: cancelHandoffs,
		running:        make(map[string]struct{}),
		cancels:        make(map[string]context.CancelFunc),
		cancelReasons:  make(map[string]string),
		idle:           idle,
	}
}

func (r *operationRunner) abortHandoffs() {
	if r != nil && r.cancelHandoffs != nil {
		r.cancelHandoffs()
	}
}

func (r *operationRunner) Start(operationID string) {
	r.startWithSource(operationID, operationStartSourceForeground)
}

func (r *operationRunner) StartRecovered(operationID string) {
	r.startWithSource(operationID, operationStartSourceRecovery)
}

func (r *operationRunner) startWithSource(operationID string, source operationStartSource) {
	if r == nil || r.deps.store == nil {
		return
	}
	r.mu.Lock()
	// Reject new operations once the service-owned context is cancelled —
	// otherwise a CreateOperation racing with Close would slip a goroutine
	// past the Wait drain. Checking Err under the same lock that guards
	// wg.Add means Close sees a consistent count.
	if r.ctx == nil || r.ctx.Err() != nil {
		r.mu.Unlock()
		return
	}
	if _, exists := r.running[operationID]; exists {
		r.mu.Unlock()
		return
	}
	if len(r.running) == 0 {
		r.idle = make(chan struct{})
	}
	opCtx, cancel := context.WithCancel(r.ctx)
	r.observeStartRequested(source)
	r.running[operationID] = struct{}{}
	r.cancels[operationID] = cancel
	r.wg.Add(1)
	r.mu.Unlock()

	go func(execCtx context.Context) {
		defer r.wg.Done()
		defer r.finish(operationID)
		_ = r.executeAndObserve(execCtx, operationID, source)
	}(opCtx)
}

func (r *operationRunner) executeAndObserve(ctx context.Context, operationID string, source operationStartSource) error {
	err := r.execute(ctx, operationID, source)
	if err == nil {
		return nil
	}
	if r.shouldRecordCancelDrainFailure(err) {
		r.recordCancelDrainFailure(operationID, planningDrainFailure(err))
	}
	if !errors.Is(err, context.Canceled) && !errors.Is(err, state.ErrPlanningLeaseConflict) {
		r.observeStartFailure(source, err)
		log.Printf("planner start source=%s operation_id=%s err=%v", source, operationID, err)
	}
	return err
}

// Only unresolved drain/cleanup failures belong to shutdown. The service may
// be cancelled after a failed operation commits but before this worker returns;
// that timing must not relabel its already-settled execution outcome.
func (r *operationRunner) shouldRecordCancelDrainFailure(err error) bool {
	if r.ctx == nil || r.ctx.Err() == nil {
		return false
	}
	return planningDrainFailure(err) != nil
}

func (r *operationRunner) recordCancelDrainFailure(operationID string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cancelDrainErr = errors.Join(r.cancelDrainErr, fmt.Errorf("cancel drain %s: %w", operationID, err))
}

// Wait blocks until every Start-launched goroutine has returned, or until
// ctx is cancelled (typically a parent shutdown deadline). It does not
// itself cancel anything — callers must cancel r.ctx (or the originating
// service-owned cancel) first if they want in-flight work to exit early.
//
// Returns the accumulated cancelDrainErr (joined per-op cancel-drain
// failures observed while the service ctx was cancelled). Service.Close
// joins this into its returned error so kingdomd's exit code reflects
// any cancel-write timeout. The error is NOT cleared after Wait returns
// — repeated Wait/Close calls remain truthful, and the runner is a
// single-use artifact tied to the service lifecycle anyway.
func (r *operationRunner) Wait(ctx context.Context) error {
	return r.WaitWithAbort(ctx, nil)
}

// WaitWithAbort is the shutdown-time variant of Wait: callers can
// supply an abort channel that short-circuits the wait when a harder
// stop supersedes graceful drain.
func (r *operationRunner) WaitWithAbort(ctx context.Context, abort <-chan struct{}) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	idle := r.idle
	r.mu.Unlock()
	select {
	case <-idle:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.cancelDrainErr
	case <-abort:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *operationRunner) finish(operationID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.running, operationID)
	if cancel := r.cancels[operationID]; cancel != nil {
		cancel()
	}
	delete(r.cancels, operationID)
	delete(r.cancelReasons, operationID)
	if len(r.running) == 0 && r.idle != nil {
		close(r.idle)
	}
}

func (r *operationRunner) RequestCancel(operationID, reason string) bool {
	if r == nil {
		return false
	}
	trimmedID := strings.TrimSpace(operationID)
	if trimmedID == "" {
		return false
	}
	trimmedReason := strings.TrimSpace(reason)
	if trimmedReason == "" {
		trimmedReason = "operation cancelled"
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cancel, ok := r.cancels[trimmedID]
	if !ok {
		return false
	}
	r.cancelReasons[trimmedID] = trimmedReason
	cancel()
	return true
}

func (r *operationRunner) CancelRecoveredOperation(ctx context.Context, operation state.PlanningOperation, reason string) error {
	next, err := r.nextOperationEventSeq(ctx, operation.OperationID)
	if err != nil {
		return err
	}
	return r.cancelOperation(ctx, operation, newSequenceCursor(next), reason)
}

func (r *operationRunner) cancelReasonOrNil(operationID string) *string {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	reason, ok := r.cancelReasons[strings.TrimSpace(operationID)]
	if !ok || strings.TrimSpace(reason) == "" {
		return nil
	}
	reason = strings.TrimSpace(reason)
	return &reason
}

func (r *operationRunner) interruptReasonOrNil(operationID string) *string {
	if reason := r.cancelReasonOrNil(operationID); reason != nil {
		return reason
	}
	return r.shutdownReasonOrNil()
}

func (r *operationRunner) execute(ctx context.Context, operationID string, source operationStartSource) error {
	claimed, ok, err := r.claimOperationWithRetry(ctx, operationID, source)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			if reason := r.cancelReasonOrNil(operationID); reason != nil {
				return r.cancelQueuedOperation(ctx, operationID, *reason)
			}
		}
		return err
	}
	if !ok {
		return nil
	}

	executionCtx, cancelExecution := context.WithCancel(ctx)
	defer cancelExecution()
	heartbeatCtx, cancelHeartbeat := context.WithCancel(executionCtx)
	defer cancelHeartbeat()
	heartbeatErrCh := r.startHeartbeat(heartbeatCtx, claimed, cancelExecution)

	execErr := r.executeClaimed(executionCtx, claimed, source)
	if r.afterExecuteClaimed != nil {
		r.afterExecuteClaimed()
	}
	cancelHeartbeat()
	heartbeatErr := <-heartbeatErrCh
	if heartbeatErr == nil {
		return execErr
	}
	if execErr != nil {
		// A heartbeat can cancel an active execution while another execution
		// error is being returned. Preserve both causes for the caller.
		return errors.Join(execErr, heartbeatErr)
	}
	// A terminal settlement clears its lease in the same transaction. A
	// renewal that races afterwards must not turn that committed success into
	// a runner failure, but a stale worker that lost its lease before
	// settlement must still report the renewal error.
	// The execution context is cancelled before this read so the heartbeat can
	// stop. This is read-only and cannot outlive DB mutation ownership.
	settled, found, lookupErr := r.deps.store.GetOperation(context.WithoutCancel(executionCtx), claimed.OperationID)
	if lookupErr == nil && found && settled.Status == state.PlanningOperationStatusSucceeded {
		return nil
	}
	return heartbeatErr
}

func (r *operationRunner) cancelQueuedOperation(ctx context.Context, operationID, reason string) error {
	if r == nil || r.deps.store == nil {
		return context.Canceled
	}
	operation, found, err := r.deps.store.GetOperation(ctx, operationID)
	if err != nil || !found {
		return err
	}
	if operation.Status != state.PlanningOperationStatusQueued {
		return context.Canceled
	}
	now := r.deps.nowString()
	terminalEvent, err := r.buildTerminalEventFromCursor(ctx, operation.OperationID, nil, acp.RunCancelledEventType, plannerMessagePayload(operation, map[string]any{"reason": reason}))
	if err != nil {
		return err
	}
	if err := r.deps.store.CancelQueuedOperation(ctx, state.CancelQueuedPlanningOperationRequest{
		OperationID:   operation.OperationID,
		ErrorMessage:  reason,
		UpdatedAt:     now,
		FinishedAt:    now,
		TerminalEvent: terminalEvent,
	}); err != nil {
		if errors.Is(err, state.ErrPlanningLeaseConflict) {
			return context.Canceled
		}
		return err
	}
	if r.deps.broker != nil {
		r.deps.broker.Publish(operation.OperationID)
	}
	return context.Canceled
}

func (r *operationRunner) failQueuedUnsupportedExternalContext(ctx context.Context, operation state.PlanningOperation, mismatch *state.UnsupportedPlanningExternalContextVersionError) error {
	if mismatch == nil {
		return fmt.Errorf("unsupported linked-source context version error is required")
	}
	now := r.deps.nowString()
	event, err := r.buildTerminalEventFromCursor(ctx, operation.OperationID, nil, acp.RunFailedEventType, plannerGenericPayload(operation, acp.KingdomProgressKind, map[string]any{"error": mismatch.Error()}))
	if err != nil {
		return err
	}
	if err := r.deps.store.FailQueuedOperation(ctx, state.FailQueuedPlanningOperationRequest{
		OperationID:   operation.OperationID,
		ErrorMessage:  mismatch.Error(),
		Failure:       plannerTurnFailure("dependency_blocked", mismatch),
		UpdatedAt:     now,
		FinishedAt:    now,
		TerminalEvent: *event,
	}); err != nil {
		if errors.Is(err, state.ErrPlanningLeaseConflict) {
			return nil
		}
		return err
	}
	if r.deps.metrics != nil {
		r.deps.metrics.failureCount.Add(1)
	}
	if r.deps.broker != nil {
		r.deps.broker.Publish(operation.OperationID)
	}
	return nil
}

func (r *operationRunner) executeClaimed(ctx context.Context, operation state.PlanningOperation, source operationStartSource) error {
	startSeq, err := r.nextOperationEventSeq(ctx, operation.OperationID)
	if err != nil {
		return err
	}
	cursor := newSequenceCursor(startSeq)

	session, ok, err := r.deps.store.GetSession(ctx, operation.PlanningSessionID)
	if err != nil {
		return err
	}
	if !ok {
		return r.failOperation(ctx, operation, cursor, fmt.Errorf("planning session not found"))
	}
	request, err := decodeOperationRequest(operation.RequestPayloadJSON)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	contextVersionMismatch, err := r.deps.store.UnsupportedPlanningOperationExternalContextVersion(ctx, operation.OperationID)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, fmt.Errorf("validate immutable linked-source context: %w", err))
	}
	if contextVersionMismatch != nil {
		return r.failOperation(ctx, operation, cursor, contextVersionMismatch)
	}

	startedPayload := map[string]any{
		"status":         acp.PlannerRunStatusInProgress,
		"operation_type": operation.Type,
		"start_source":   source,
	}
	if queueAgeMs := queueAgeMilliseconds(operation); queueAgeMs >= 0 {
		startedPayload["queue_age_ms"] = queueAgeMs
	}
	payload := plannerRunPayload(operation.OperationID, operation.PlanningSessionID, operation.Type)
	for key, value := range startedPayload {
		payload[key] = value
	}
	if _, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.RunInProgressEventType, payload); err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}

	switch operation.Type {
	case state.PlanningOperationTypeReply:
		return r.executeReplyOperation(ctx, session, operation, request, cursor)
	case state.PlanningOperationTypeFormalize:
		return r.executeFormalizeOperation(ctx, session, operation, request, cursor)
	case state.PlanningOperationTypeRegenerate:
		return r.executeRegenerateOperation(ctx, session, operation, request, cursor)
	case state.PlanningOperationTypeGenerateTitle:
		return r.executeGenerateTitleOperation(ctx, session, operation, cursor)
	default:
		return r.failOperation(ctx, operation, cursor, fmt.Errorf("unsupported planner operation type: %q", operation.Type))
	}
}

func (r *operationRunner) executeReplyOperation(ctx context.Context, session state.PlanningSession, operation state.PlanningOperation, request operationRequest, cursor *sequenceCursor) (resultErr error) {
	if r.deps.planner == nil {
		return r.failOperation(ctx, operation, cursor, fmt.Errorf("planning agent is not configured"))
	}
	messages, err := r.deps.store.ActiveMessageSource(ctx, session.PlanningSessionID)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	forceNewSession := request.ForceNewSession || operation.AutomaticRetry || requiresFreshProviderSession(ctx, r.deps.planner, operation.OperationID)
	providerSessionID := ""
	if !forceNewSession {
		providerSessionID, err = providerSessionIDForPlanningInvocation(ctx, r.deps.store, session, operation, true)
		if err != nil {
			return r.failOperation(ctx, operation, cursor, err)
		}
	}
	// An empty durable id means this invocation must create a new provider
	// session. Never let the adapter's in-memory cache become implicit reuse
	// authority for a planner invocation.
	forceNewSession = forceNewSession || strings.TrimSpace(providerSessionID) == ""
	agentContext, err := r.deps.agentContext(ctx, operation, session, messages, forceNewSession)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	agentContext.CurrentMessageID = request.UserMessageID
	providerInvocation, err := r.createPlanningProviderInvocation(ctx, session, operation, providerSessionID)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}

	r.decorateProviderInvocationContext(ctx, operation, cursor, providerInvocation)
	textWriter := newBufferedOperationTextWriter(ctx, operation.OperationID, cursor, streamingMessagePartPayload(operation, request), r.appendOperationEvent)
	agentContext.ProviderInvocation = providerInvocation
	turn, err := r.deps.planner.ContinueTurnStream(ctx, agentContext, func(text string) {
		textWriter.Write(text)
	})
	adoptActivation(&operation, providerInvocation)
	metadata := resultProviderMetadata(turn.Metadata, err)
	failAfterProvider := func(cause error) error {
		return r.failOperation(ctx, operation, cursor, executionError(metadata, cause))
	}
	defer func() {
		resultErr = errors.Join(resultErr, r.finishProviderContent(operation, metadata, turn.Close))
	}()
	if flushErr := textWriter.Close(); err == nil && flushErr != nil {
		err = executionError(metadata, flushErr)
	}
	// Shutdown short-circuit: if the service ctx was cancelled while the
	// planner was running, finalize as cancelled regardless of how the
	// planner returned. A cooperative planner returns ctx.Canceled here;
	// an uncooperative one might still return success after shutdown,
	// and writing a `done` terminal event for an operation the user
	// expected to be cancelled would lie. Same pattern repeats after
	// every long planner call below.
	if reason := r.interruptReasonOrNil(operation.OperationID); reason != nil {
		return r.cancelOperationWithMetadata(ctx, operation, cursor, *reason, resultProviderMetadata(turn.Metadata, err))
	}
	if err != nil {
		return failAfterProvider(err)
	}
	if err := requirePlannerTurnSettled(turn.Metadata); err != nil {
		return r.failOperationWithObservedOutput(ctx, operation, cursor, executionError(turn.Metadata, err), turn.Metadata.FinalText)
	}
	var autoTitleSession *state.PlanningSession
	autoTitleSession, err = maybeAutoGenerateTitleForReplyFinalize(ctx, r.deps, r, session, messages, request, operation.OperationID, titleTriggerSourceAutoSecondUserMessageFinalize)
	if err != nil {
		log.Printf("planner auto title source=%s session_id=%s err=%v", titleTriggerSourceAutoSecondUserMessageFinalize, session.PlanningSessionID, err)
		autoTitleSession = nil
	}
	if _, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.GenericEventType, plannerPhasePayload(operation, "finalizing", nil)); err != nil {
		return failAfterProvider(err)
	}
	questionPayloadJSON, err := marshalQuestion(turn.Question)
	if err != nil {
		return failAfterProvider(err)
	}

	assistantMessage := state.AppendPlanningMessageRequest{
		ContentSource:       turn.ContentSource,
		PlanningMessageID:   request.AssistantMessageID,
		PlanningSessionID:   session.PlanningSessionID,
		ParentMessageID:     request.UserMessageID,
		Role:                state.PlanningMessageRoleAssistant,
		ContentMarkdown:     turn.ContentMarkdown,
		QuestionPayloadJSON: questionPayloadJSON,
		AnswerPayloadJSON:   `{}`,
		CreatedAt:           request.AssistantCreatedAt,
	}
	messageCompletedPayload := planningMessagePayload(state.PlanningMessage{
		PlanningMessageID:   assistantMessage.PlanningMessageID,
		PlanningSessionID:   assistantMessage.PlanningSessionID,
		ParentMessageID:     &request.UserMessageID,
		Role:                assistantMessage.Role,
		ContentMarkdown:     assistantMessage.ContentMarkdown,
		QuestionPayloadJSON: assistantMessage.QuestionPayloadJSON,
		AnswerPayloadJSON:   assistantMessage.AnswerPayloadJSON,
		CreatedAt:           assistantMessage.CreatedAt,
	})
	messageCompletedPayload = plannerMessagePayload(operation, messageCompletedPayload)
	messageCompletedPayload["text"] = assistantMessage.ContentMarkdown
	if autoTitleSession != nil {
		messageCompletedPayload["session_patch"] = sessionPatchPayload(*autoTitleSession)
	}
	messageCompletedEvent, err := r.buildTerminalEvent(operation.OperationID, cursor, acp.MessageDoneEventType, messageCompletedPayload)
	if err != nil {
		return failAfterProvider(err)
	}
	bindPlannerBodyEvent(messageCompletedEvent, turn.ContentSource, state.PlanningMessageContentID(assistantMessage.PlanningMessageID))
	runCompletedPayload := plannerMessagePayload(operation, map[string]any{
		"result_ref":          request.AssistantMessageID,
		"planning_message_id": request.AssistantMessageID,
	})
	if autoTitleSession != nil {
		runCompletedPayload["session_patch"] = sessionPatchPayload(*autoTitleSession)
	}
	terminalEvent, err := r.buildTerminalEvent(operation.OperationID, cursor, acp.RunCompletedEventType, runCompletedPayload)
	if err != nil {
		return failAfterProvider(err)
	}
	now := r.deps.nowString()
	// The user message was appended at create time; finalize appends the
	// assistant under it and moves the active leaf onto the new reply.
	finalizeReq := state.FinalizeOperationRequest{
		Kind:                  state.FinalizeReply,
		SessionID:             session.PlanningSessionID,
		OperationID:           operation.OperationID,
		LeaseOwner:            operation.LeaseOwner,
		LeaseVersion:          operation.LeaseVersion,
		ConversationState:     state.PlanningSessionConversationStateReady,
		UpdatedAt:             now,
		FinishedAt:            now,
		AssistantMessage:      &assistantMessage,
		ActiveLeafMessageID:   request.AssistantMessageID,
		ResultRef:             request.AssistantMessageID,
		ProviderSettlement:    providerSettlementForSuccess(providerInvocation.InvocationID, turn.Metadata, now),
		MessageCompletedEvent: messageCompletedEvent,
		TerminalEvent:         terminalEvent,
	}
	if err := r.deps.store.FinalizeOperation(ctx, finalizeReq); err != nil {
		return r.failSuccessfulFinalization(ctx, operation, cursor, turn.Metadata, err)
	}
	if r.deps.broker != nil {
		r.deps.broker.Publish(operation.OperationID)
	}
	return nil
}

func (r *operationRunner) executeRegenerateOperation(ctx context.Context, session state.PlanningSession, operation state.PlanningOperation, request operationRequest, cursor *sequenceCursor) (resultErr error) {
	if r.deps.planner == nil {
		return r.failOperation(ctx, operation, cursor, fmt.Errorf("planning agent is not configured"))
	}
	messages, err := r.deps.store.ActiveMessageSource(ctx, session.PlanningSessionID)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	// Regenerate starts an independent provider session for the new branch; the
	// resulting durable provider session becomes the latest reusable reply
	// session after successful settlement.
	forceNewSession := true
	agentContext, err := r.deps.agentContext(ctx, operation, session, messages, forceNewSession || request.ForceNewSession || operation.AutomaticRetry)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	providerInvocation, err := r.createPlanningProviderInvocation(ctx, session, operation, "")
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}

	r.decorateProviderInvocationContext(ctx, operation, cursor, providerInvocation)
	textWriter := newBufferedOperationTextWriter(ctx, operation.OperationID, cursor, streamingMessagePartPayload(operation, request), r.appendOperationEvent)
	agentContext.ProviderInvocation = providerInvocation
	turn, err := r.deps.planner.ContinueTurnStream(ctx, agentContext, func(text string) {
		textWriter.Write(text)
	})
	adoptActivation(&operation, providerInvocation)
	metadata := resultProviderMetadata(turn.Metadata, err)
	failAfterProvider := func(cause error) error {
		return r.failOperation(ctx, operation, cursor, executionError(metadata, cause))
	}
	defer func() {
		resultErr = errors.Join(resultErr, r.finishProviderContent(operation, metadata, turn.Close))
	}()
	if flushErr := textWriter.Close(); err == nil && flushErr != nil {
		err = executionError(metadata, flushErr)
	}
	if reason := r.interruptReasonOrNil(operation.OperationID); reason != nil {
		return r.cancelOperationWithMetadata(ctx, operation, cursor, *reason, resultProviderMetadata(turn.Metadata, err))
	}
	if err != nil {
		return failAfterProvider(err)
	}
	if err := requirePlannerTurnSettled(turn.Metadata); err != nil {
		return r.failOperationWithObservedOutput(ctx, operation, cursor, executionError(turn.Metadata, err), turn.Metadata.FinalText)
	}
	if _, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.GenericEventType, plannerPhasePayload(operation, "finalizing", nil)); err != nil {
		return failAfterProvider(err)
	}
	regenQuestionPayloadJSON, err := marshalQuestion(turn.Question)
	if err != nil {
		return failAfterProvider(err)
	}

	regenParentID := nilIfEmpty(request.ParentMessageID)
	assistantMessage := state.AppendPlanningMessageRequest{
		ContentSource:       turn.ContentSource,
		PlanningMessageID:   request.AssistantMessageID,
		PlanningSessionID:   session.PlanningSessionID,
		ParentMessageID:     request.ParentMessageID,
		Role:                state.PlanningMessageRoleAssistant,
		ContentMarkdown:     turn.ContentMarkdown,
		QuestionPayloadJSON: regenQuestionPayloadJSON,
		AnswerPayloadJSON:   `{}`,
		CreatedAt:           request.AssistantCreatedAt,
	}
	messageCompletedPayload := planningMessagePayload(state.PlanningMessage{
		PlanningMessageID:   assistantMessage.PlanningMessageID,
		PlanningSessionID:   assistantMessage.PlanningSessionID,
		ParentMessageID:     regenParentID,
		Role:                assistantMessage.Role,
		ContentMarkdown:     assistantMessage.ContentMarkdown,
		QuestionPayloadJSON: assistantMessage.QuestionPayloadJSON,
		AnswerPayloadJSON:   assistantMessage.AnswerPayloadJSON,
		CreatedAt:           assistantMessage.CreatedAt,
	})
	messageCompletedPayload = plannerMessagePayload(operation, messageCompletedPayload)
	messageCompletedPayload["text"] = assistantMessage.ContentMarkdown
	messageCompletedEvent, err := r.buildTerminalEvent(operation.OperationID, cursor, acp.MessageDoneEventType, messageCompletedPayload)
	if err != nil {
		return failAfterProvider(err)
	}
	bindPlannerBodyEvent(messageCompletedEvent, turn.ContentSource, state.PlanningMessageContentID(assistantMessage.PlanningMessageID))
	terminalEvent, err := r.buildTerminalEvent(operation.OperationID, cursor, acp.RunCompletedEventType, plannerMessagePayload(operation, map[string]any{
		"result_ref":          request.AssistantMessageID,
		"planning_message_id": request.AssistantMessageID,
	}))
	if err != nil {
		return failAfterProvider(err)
	}
	now := r.deps.nowString()
	if err := r.deps.store.FinalizeOperation(ctx, state.FinalizeOperationRequest{
		Kind:                  state.FinalizeRegenerate,
		SessionID:             session.PlanningSessionID,
		OperationID:           operation.OperationID,
		LeaseOwner:            operation.LeaseOwner,
		LeaseVersion:          operation.LeaseVersion,
		ConversationState:     state.PlanningSessionConversationStateReady,
		UpdatedAt:             now,
		FinishedAt:            now,
		AssistantMessage:      &assistantMessage,
		ActiveLeafMessageID:   request.AssistantMessageID,
		ResultRef:             request.AssistantMessageID,
		ProviderSettlement:    providerSettlementForSuccess(providerInvocation.InvocationID, turn.Metadata, now),
		MessageCompletedEvent: messageCompletedEvent,
		TerminalEvent:         terminalEvent,
	}); err != nil {
		return r.failSuccessfulFinalization(ctx, operation, cursor, turn.Metadata, err)
	}
	if r.deps.broker != nil {
		r.deps.broker.Publish(operation.OperationID)
	}
	return nil
}

func (r *operationRunner) executeFormalizeOperation(ctx context.Context, session state.PlanningSession, operation state.PlanningOperation, request operationRequest, cursor *sequenceCursor) (resultErr error) {
	if r.deps.planner == nil {
		return r.failOperation(ctx, operation, cursor, fmt.Errorf("planning agent is not configured"))
	}
	messages, err := r.deps.store.ActiveMessageSource(ctx, session.PlanningSessionID)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	forceNewSession := operation.AutomaticRetry || requiresFreshProviderSession(ctx, r.deps.planner, operation.OperationID)
	providerSessionID := ""
	if !forceNewSession {
		providerSessionID, err = providerSessionIDForPlanningInvocation(ctx, r.deps.store, session, operation, true)
		if err != nil {
			return r.failOperation(ctx, operation, cursor, err)
		}
	}
	forceNewSession = forceNewSession || strings.TrimSpace(providerSessionID) == ""
	agentContext, err := r.deps.agentContext(ctx, operation, session, messages, forceNewSession)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	providerInvocation, err := r.createPlanningProviderInvocation(ctx, session, operation, providerSessionID)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	r.decorateProviderInvocationContext(ctx, operation, cursor, providerInvocation)
	textWriter := newBufferedOperationTextWriter(ctx, operation.OperationID, cursor, streamingMessagePartPayload(operation, request), r.appendOperationEvent)
	agentContext.ProviderInvocation = providerInvocation
	plan, err := r.deps.planner.FormalizePlanStream(ctx, agentContext, func(text string) {
		textWriter.Write(text)
	})
	adoptActivation(&operation, providerInvocation)
	metadata := resultProviderMetadata(plan.Metadata, err)
	failAfterProvider := func(cause error) error {
		return r.failOperation(ctx, operation, cursor, executionError(metadata, cause))
	}
	defer func() {
		resultErr = errors.Join(resultErr, r.finishProviderContent(operation, metadata, plan.Close))
	}()
	if flushErr := textWriter.Close(); err == nil && flushErr != nil {
		err = executionError(metadata, flushErr)
	}
	if reason := r.interruptReasonOrNil(operation.OperationID); reason != nil {
		return r.cancelOperationWithMetadata(ctx, operation, cursor, *reason, resultProviderMetadata(plan.Metadata, err))
	}
	if err != nil {
		return failAfterProvider(err)
	}
	if err := requirePlannerTurnSettled(plan.Metadata); err != nil {
		return r.failOperationWithObservedOutput(ctx, operation, cursor, executionError(plan.Metadata, err), plan.Metadata.FinalText)
	}
	if _, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.GenericEventType, plannerPhasePayload(operation, "finalizing", nil)); err != nil {
		return failAfterProvider(err)
	}
	// Drafts no longer carry a hand-crafted summary placeholder; the WorkOrder
	// Plan body itself is the canonical summary. We persist an empty JSON
	// object to satisfy the NOT NULL constraint without lying to the client.
	summaryJSON := []byte(`{}`)
	draft := state.CreatePlanningDraftRequest{
		TextSource:        plan.TextSource,
		PlanningDraftID:   request.DraftID,
		PlanningSessionID: session.PlanningSessionID,
		WorkOrderTitle:    firstNonEmpty(plan.WorkOrderTitle, session.Title),
		WorkOrderText:     plan.WorkOrderText,
		SummaryJSON:       string(summaryJSON),
		CreatedAt:         request.DraftCreatedAt,
	}
	messageCompletedEvent, err := r.buildTerminalEvent(operation.OperationID, cursor, acp.MessageDoneEventType, plannerMessagePayload(operation, map[string]any{
		"content_markdown":  draft.WorkOrderText,
		"text":              draft.WorkOrderText,
		"work_order_title":  draft.WorkOrderTitle,
		"planning_draft_id": draft.PlanningDraftID,
		"created_at":        draft.CreatedAt,
	}))
	if err != nil {
		return failAfterProvider(err)
	}
	bindPlannerBodyEvent(messageCompletedEvent, plan.TextSource, state.PlanningDraftContentID(draft.PlanningDraftID))
	terminalEvent, err := r.buildTerminalEvent(operation.OperationID, cursor, acp.RunCompletedEventType, plannerMessagePayload(operation, map[string]any{
		"result_ref":        draft.PlanningDraftID,
		"planning_draft_id": draft.PlanningDraftID,
	}))
	if err != nil {
		return failAfterProvider(err)
	}
	now := r.deps.nowString()
	if err := r.deps.store.FinalizeOperation(ctx, state.FinalizeOperationRequest{
		Kind:                  state.FinalizeFormalize,
		SessionID:             session.PlanningSessionID,
		OperationID:           operation.OperationID,
		LeaseOwner:            operation.LeaseOwner,
		LeaseVersion:          operation.LeaseVersion,
		ConversationState:     state.PlanningSessionConversationStateReady,
		UpdatedAt:             now,
		FinishedAt:            now,
		Draft:                 &draft,
		CurrentDraftID:        draft.PlanningDraftID,
		ResultRef:             draft.PlanningDraftID,
		ProviderSettlement:    providerSettlementForSuccess(providerInvocation.InvocationID, plan.Metadata, now),
		MessageCompletedEvent: messageCompletedEvent,
		TerminalEvent:         terminalEvent,
	}); err != nil {
		return r.failSuccessfulFinalization(ctx, operation, cursor, plan.Metadata, err)
	}
	if r.deps.broker != nil {
		r.deps.broker.Publish(operation.OperationID)
	}
	return nil
}

func (r *operationRunner) executeGenerateTitleOperation(ctx context.Context, session state.PlanningSession, operation state.PlanningOperation, cursor *sequenceCursor) (resultErr error) {
	if r.deps.planner == nil {
		return r.failOperation(ctx, operation, cursor, fmt.Errorf("planning agent is not configured"))
	}
	messages, err := r.deps.store.ActiveMessageSource(ctx, session.PlanningSessionID)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	agentContext, err := r.deps.agentContext(ctx, operation, session, messages, true)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	providerInvocation, err := r.createPlanningProviderInvocation(ctx, session, operation, "")
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	r.decorateProviderInvocationContext(ctx, operation, cursor, providerInvocation)
	textWriter := newBufferedOperationTextWriter(ctx, operation.OperationID, cursor, streamingMessagePartPayload(operation, operationRequest{}), r.appendOperationEvent)
	agentContext.ProviderInvocation = providerInvocation
	generatedTitle, err := r.deps.planner.GenerateTitle(ctx, agentContext, func(text string) {
		textWriter.Write(text)
	})
	adoptActivation(&operation, providerInvocation)
	metadata := resultProviderMetadata(generatedTitle.Metadata, err)
	failAfterProvider := func(cause error) error {
		return r.failOperation(ctx, operation, cursor, executionError(metadata, cause))
	}
	defer func() { resultErr = errors.Join(resultErr, r.retireProviderContent(operation, metadata)) }()
	if flushErr := textWriter.Close(); err == nil && flushErr != nil {
		err = executionError(metadata, flushErr)
	}
	if reason := r.interruptReasonOrNil(operation.OperationID); reason != nil {
		return r.cancelOperationWithMetadata(ctx, operation, cursor, *reason, resultProviderMetadata(generatedTitle.Metadata, err))
	}
	if err != nil {
		return failAfterProvider(err)
	}
	if err := requirePlannerTurnSettled(generatedTitle.Metadata); err != nil {
		return r.failOperationWithObservedOutput(ctx, operation, cursor, executionError(generatedTitle.Metadata, err), generatedTitle.Metadata.FinalText)
	}
	if _, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.GenericEventType, plannerPhasePayload(operation, "finalizing", nil)); err != nil {
		return failAfterProvider(err)
	}
	finalTitle := strings.TrimSpace(generatedTitle.Title)
	if finalTitle == "" {
		return failAfterProvider(fmt.Errorf("generated title is empty"))
	}
	now := r.deps.nowString()
	messageCompletedEvent, err := r.buildTerminalEvent(operation.OperationID, cursor, acp.MessageDoneEventType, plannerMessagePayload(operation, map[string]any{
		"content_markdown": finalTitle,
		"text":             finalTitle,
		"title":            finalTitle,
		"title_state":      "generated",
		"created_at":       now,
	}))
	if err != nil {
		return failAfterProvider(err)
	}
	terminalEvent, err := r.buildTerminalEvent(operation.OperationID, cursor, acp.RunCompletedEventType, plannerMessagePayload(operation, map[string]any{
		"result_ref":  finalTitle,
		"title":       finalTitle,
		"title_state": "generated",
	}))
	if err != nil {
		return failAfterProvider(err)
	}
	if err := r.deps.store.FinalizeGenerateTitle(ctx, state.FinalizeGenerateTitleRequest{
		SessionID:             session.PlanningSessionID,
		OperationID:           operation.OperationID,
		LeaseOwner:            operation.LeaseOwner,
		LeaseVersion:          operation.LeaseVersion,
		UpdatedAt:             now,
		FinishedAt:            now,
		GeneratedTitle:        finalTitle,
		ObservedOutputText:    finalTitle,
		ProviderSettlement:    providerSettlementForSuccess(providerInvocation.InvocationID, generatedTitle.Metadata, now),
		MessageCompletedEvent: messageCompletedEvent,
		TerminalEvent:         terminalEvent,
	}); err != nil {
		return r.failSuccessfulFinalization(ctx, operation, cursor, generatedTitle.Metadata, err)
	}
	if r.deps.broker != nil {
		r.deps.broker.Publish(operation.OperationID)
	}
	return nil
}

func (r *operationRunner) failOperation(ctx context.Context, operation state.PlanningOperation, cursor *sequenceCursor, cause error) error {
	return r.failOperationWithObservedOutput(ctx, operation, cursor, cause, providerMetadataFromError(cause).FinalText)
}

func (r *operationRunner) failSuccessfulFinalization(ctx context.Context, operation state.PlanningOperation, cursor *sequenceCursor, metadata ProviderExecutionMetadata, cause error) error {
	if cause == nil {
		return nil
	}
	if operation.ActiveProviderInvocationID != nil {
		readbackCtx, cancel := context.WithTimeout(r.handoffCtx, planningOperationCommitReadbackTimeout)
		status, confirmed, err := r.deps.store.ConfirmPlanningProviderContentIngestion(readbackCtx, operation.OperationID, *operation.ActiveProviderInvocationID, metadata.StreamObservation)
		cancel()
		if err != nil {
			return errors.Join(cause, fmt.Errorf("resolve planner finalization outcome: %w", err))
		}
		if confirmed && status == state.PlanningOperationStatusSucceeded {
			if r.deps.broker != nil {
				r.deps.broker.Publish(operation.OperationID)
			}
			return nil
		}
	}
	return r.failOperationWithObservedOutput(
		ctx,
		operation,
		cursor,
		executionError(metadata, fmt.Errorf("finalize successful planner operation: %w", cause)),
		metadata.FinalText,
	)
}

func (r *operationRunner) failOperationWithObservedOutput(ctx context.Context, operation state.PlanningOperation, cursor *sequenceCursor, cause error, observedOutputText string) error {
	// Shutdown short-circuit: if the service-owned context was
	// cancelled, this is a system-driven termination, not a planner
	// failure. Re-route to cancelOperation so the row lands in
	// `cancelled` (not `failed`), the failure metric isn't bumped,
	// and the terminal write uses a decoupled context that survives
	// the cancelled service ctx. One edit absorbs every pre-planner
	// DB error path, the bootstrap path, and the post-planner failure
	// path — anywhere failOperation gets called during shutdown
	// finalises consistently as cancelled instead of leaving the row
	// in `running` for next-startup recovery to clean up.
	if reason := r.interruptReasonOrNil(operation.OperationID); reason != nil {
		return r.cancelOperationWithMetadata(ctx, operation, cursor, *reason, providerMetadataFromError(cause))
	}
	if cause == nil {
		cause = fmt.Errorf("planner operation failed")
	}
	// A provider request that ended without terminal evidence settles as
	// abandoned now, rather than when recovery finds the operation idle.
	if operation.ActiveProviderInvocationID != nil && providerMetadataFromError(cause).StreamObservation.Request == nil {
		abandoned, release, err := r.deps.abandonUnprovenRequest(ctx, &operation, cause)
		if err != nil {
			return errors.Join(cause, err)
		}
		defer release()
		cause = abandoned
	}
	now := r.deps.nowString()
	providerMetadata := providerMetadataFromError(cause)
	observedOutputText = firstNonEmpty(strings.TrimSpace(observedOutputText), strings.TrimSpace(providerMetadata.FinalText))
	if observedOutputText != "" && !providerMetadata.StreamObservation.MeaningfulProgress {
		providerMetadata.FinalText = observedOutputText
		providerMetadata.StreamObservation.MeaningfulProgress = true
		cause = executionError(providerMetadata, cause)
	}
	observedOutputText = sanitizeObservedPlannerOutput(operation.Type, observedOutputText)
	finalizeErr := error(nil)
	if operation.ActiveProviderInvocationID != nil && r.deps.retryEngine != nil {
		// Retry-aware finalization chooses and persists its one terminal event
		// after the durable retry decision is known.
		_, finalizeErr = r.finalizeFailureWithRetry(ctx, operation, cursor, cause, observedOutputText, false)
	} else {
		terminalEvent, eventErr := r.buildTerminalEventFromCursor(ctx, operation.OperationID, cursor, acp.RunFailedEventType, plannerGenericPayload(operation, acp.KingdomProgressKind, map[string]any{"error": cause.Error()}))
		if eventErr != nil {
			return eventErr
		}
		providerSettlement := providerSettlementForFailure(operation, providerMetadata, now, cause.Error(), cause, false)
		failure := plannerTurnFailure(string(canonicalPlanningRetryCode(failureReasonCodeForPlannerFailure(providerMetadata, cause, false), providerMetadata)), cause)
		finalizeErr = r.finalizeAbortOperation(ctx, operation, state.PlanningOperationStatusFailed, now, cause.Error(), observedOutputText, providerSettlement, terminalEvent, failure, false)
	}
	if finalizeErr != nil {
		// A concurrent finaliser (recovery vs SSE-observer, or any
		// other observer that already finalised this op) winning the
		// lease race is benign — the op is terminal either way.
		// Bumping failureCount on the loser inflates failure-rate
		// alerts.
		if errors.Is(finalizeErr, state.ErrPlanningLeaseConflict) {
			return nil
		}
		return finalizeErr
	}
	if r.deps.metrics != nil {
		r.deps.metrics.failureCount.Add(1)
	}
	if r.deps.broker != nil {
		r.deps.broker.Publish(operation.OperationID)
	}
	return &settledPlanningFailure{cause: cause}
}

// cancelOperation finalizes an in-flight operation as cancelled (status =
// cancelled), distinct from failOperation. Used when the runner observed
// shutdown cancellation on its service-owned context — the operation is
// not a planner failure, just a system-driven interruption, so it must
// not feed the failure metric or land on a UI surface that reads as
// "something went wrong".
//
// The terminal DB write uses the runner-owned, revocable handoff context plus
// a bounded deadline. It can record cancellation after execution cancellation,
// but Service.Close revokes it at its own deadline so it cannot outlive DB
// ownership.
//
// If the bounded write times out, the row stays `running` and this
// function returns the wrapped DeadlineExceeded. The runner goroutine
// wrapper records that via cancelDrainErr; runner.Wait surfaces it to
// Service.Close which surfaces it to kingdomd's exit code (exitDrainFailure).
// Recovery on the next boot picks up the stuck row via the 3× TTL
// stuck-finalise path. Operators see the failure on the wire instead of
// having to scrape logs.
func (r *operationRunner) cancelOperation(ctx context.Context, operation state.PlanningOperation, cursor *sequenceCursor, reason string) error {
	return r.cancelOperationWithMetadata(ctx, operation, cursor, reason, ProviderExecutionMetadata{})
}

func resultProviderMetadata(metadata ProviderExecutionMetadata, err error) ProviderExecutionMetadata {
	var executionErr *AgentExecutionError
	if errors.As(err, &executionErr) {
		return executionErr.Metadata
	}
	return metadata
}

func (r *operationRunner) cancelOperationWithMetadata(ctx context.Context, operation state.PlanningOperation, cursor *sequenceCursor, reason string, metadata ProviderExecutionMetadata) error {
	if strings.TrimSpace(reason) == "" {
		reason = "operation cancelled"
	}
	writeCtx, cancelWrite := context.WithTimeout(r.handoffCtx, terminalCancelWriteTimeout)
	defer cancelWrite()
	payload := plannerMessagePayload(operation, map[string]any{"reason": reason})
	terminalEvent, eventErr := r.buildTerminalEventFromCursor(writeCtx, operation.OperationID, cursor, acp.RunCancelledEventType, payload)
	if eventErr != nil {
		return eventErr
	}
	now := r.deps.nowString()
	// The runtime requested cancellation even if the provider completed before
	// it received the interrupt. Preserve that raw terminal outcome separately.
	metadata.StreamObservation.Cancelled = true
	providerSettlement := providerSettlementForFailure(operation, metadata, now, reason, nil, true)
	if err := r.finalizeAbortOperation(writeCtx, operation, state.PlanningOperationStatusCancelled, now, reason, "", providerSettlement, terminalEvent, nil, false); err != nil {
		// Force/deadline revocation intentionally forbids further handoff
		// writes. It is distinct from an unsuccessful guardian cancellation.
		if r.handoffCtx.Err() == context.Canceled && onlyContextCancellation(err) {
			return context.Canceled
		}
		return err
	}
	if r.deps.broker != nil {
		r.deps.broker.Publish(operation.OperationID)
	}
	// Returning context.Canceled keeps execute()'s error chain honest
	// (the goroutine was cancelled), but the row is durably cancelled
	// regardless of whether anything observes the return value.
	return context.Canceled
}

// finalizeAbortOperation ends an operation as failed or cancelled. failure
// explains a failed turn and is nil for a cancellation.
func (r *operationRunner) finalizeAbortOperation(ctx context.Context, operation state.PlanningOperation, status state.PlanningOperationStatus, now, reason, observedOutputText string, providerSettlement *state.FinalizePlanningProviderSettlementRequest, terminalEvent *state.AppendPlanningOperationEventRequest, failure *state.PlanningTurnFailure, allowExpiredLease bool) error {
	switch operation.Type {
	case state.PlanningOperationTypeGenerateTitle:
		return r.deps.store.FinalizeGenerateTitleAbort(ctx, state.FinalizeGenerateTitleAbortRequest{
			SessionID:          operation.PlanningSessionID,
			OperationID:        operation.OperationID,
			LeaseOwner:         operation.LeaseOwner,
			LeaseVersion:       operation.LeaseVersion,
			Status:             status,
			UpdatedAt:          now,
			FinishedAt:         now,
			ErrorMessage:       reason,
			ObservedOutputText: observedOutputText,
			AllowExpiredLease:  allowExpiredLease,
			ProviderSettlement: providerSettlement,
			TerminalEvent:      terminalEvent,
		})
	default:
		kind := state.FinalizeFail
		if status == state.PlanningOperationStatusCancelled {
			kind = state.FinalizeCancel
		}
		return r.deps.store.FinalizeOperation(ctx, state.FinalizeOperationRequest{
			Kind:               kind,
			SessionID:          operation.PlanningSessionID,
			OperationID:        operation.OperationID,
			LeaseOwner:         operation.LeaseOwner,
			LeaseVersion:       operation.LeaseVersion,
			ConversationState:  state.PlanningSessionConversationStateReady,
			UpdatedAt:          now,
			FinishedAt:         now,
			ErrorMessage:       reason,
			Failure:            failure,
			ObservedOutputText: observedOutputText,
			AllowExpiredLease:  allowExpiredLease,
			ProviderSettlement: providerSettlement,
			TerminalEvent:      terminalEvent,
		})
	}
}

// shutdownReasonOrNil returns a stable cancellation reason iff the
// runner's service-owned context has been cancelled, else nil. Callers
// use it as the post-planner guard: if non-nil, the operation must
// finalize as cancelled regardless of how the planner returned —
// cooperative planners return ctx.Canceled and a stray success after
// shutdown would otherwise lie about a result the user expected to be
// cancelled.
func (r *operationRunner) shutdownReasonOrNil() *string {
	if r == nil || r.ctx == nil {
		return nil
	}
	if r.ctx.Err() == nil {
		return nil
	}
	reason := "operation cancelled during shutdown"
	return &reason
}

func (r *operationRunner) nextOperationEventSeq(ctx context.Context, operationID string) (int, error) {
	latest, ok, err := r.deps.store.GetLatestOperationEvent(ctx, operationID)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 1, nil
	}
	return latest.Seq + 1, nil
}

func (r *operationRunner) appendOperationEvent(ctx context.Context, operationID string, cursor *sequenceCursor, eventType string, payload any) (state.PlanningOperationEvent, error) {
	return r.appendOperationEventContent(ctx, operationID, cursor, eventType, payload, nil)
}

func (r *operationRunner) appendOperationEventContent(ctx context.Context, operationID string, cursor *sequenceCursor, eventType string, payload any, sources map[string]providerexecution.ActivityContent) (state.PlanningOperationEvent, error) {
	if cursor == nil {
		return state.PlanningOperationEvent{}, fmt.Errorf("operation event cursor is required")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return state.PlanningOperationEvent{}, fmt.Errorf("marshal planning operation event payload: %w", err)
	}
	seq := cursor.Reserve()
	event, err := r.deps.store.AppendOperationEvent(ctx, state.AppendPlanningOperationEventRequest{
		EventID:          fmt.Sprintf("%s:%d", operationID, seq),
		OperationID:      operationID,
		Seq:              seq,
		EventType:        strings.TrimSpace(eventType),
		PayloadJSON:      string(raw),
		ActivityContents: sources,
		CreatedAt:        r.deps.nowString(),
	})
	if err != nil {
		return state.PlanningOperationEvent{}, err
	}
	if r.deps.broker != nil {
		r.deps.broker.Publish(operationID)
	}
	return event, nil
}

func (r *operationRunner) buildTerminalEvent(operationID string, cursor *sequenceCursor, eventType string, payload any) (*state.AppendPlanningOperationEventRequest, error) {
	if cursor == nil {
		return nil, fmt.Errorf("operation event cursor is required")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal planning operation event payload: %w", err)
	}
	seq := cursor.Reserve()
	return &state.AppendPlanningOperationEventRequest{
		EventID:     fmt.Sprintf("%s:%d", operationID, seq),
		OperationID: operationID,
		Seq:         seq,
		EventType:   strings.TrimSpace(eventType),
		PayloadJSON: string(raw),
		CreatedAt:   r.deps.nowString(),
	}, nil
}

func (r *operationRunner) decorateProviderInvocationContext(ctx context.Context, operation state.PlanningOperation, cursor *sequenceCursor, invocation *ProviderInvocationContext) {
	if r == nil || invocation == nil || cursor == nil {
		return
	}
	_, _ = r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.GenericEventType, plannerPhasePayload(operation, "provider_launching", nil))
	originalProviderSessionOpened := invocation.OnProviderSessionOpened
	invocation.OnProviderSessionOpened = func(providerSessionID string) {
		if originalProviderSessionOpened != nil {
			originalProviderSessionOpened(providerSessionID)
		}
		_, _ = r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.GenericEventType, plannerPhasePayload(operation, "provider_session_open", map[string]any{"provider_session_id": strings.TrimSpace(providerSessionID)}))
	}
	originalActivity := invocation.OnActivity
	invocation.OnActivity = func(activity ProviderActivity) {
		if originalActivity != nil {
			originalActivity(activity)
		}
		activity.InvocationID = invocation.InvocationID
		activityID := acp.ActivityIdentity(activity, r.deps.nextID("planning_activity"))
		payload := plannerActivityPayload(operation, activityID, activity)
		if len(payload) == 0 {
			return
		}
		if _, err := r.appendOperationEventContent(ctx, operation.OperationID, cursor, acp.GenericEventType, payload, activity.ContentSources()); err != nil {
			log.Printf("persist planner activity for %s: %v", operation.OperationID, err)
		}
	}
}

func plannerActivityPayload(operation state.PlanningOperation, activityID string, activity ProviderActivity) map[string]any {
	base := plannerRunPayload(operation.OperationID, operation.PlanningSessionID, operation.Type)
	return acp.ActivityPayload(base, activityID, activity)
}

func providerSessionIDForPlanningInvocation(ctx context.Context, store *state.PlanningStore, session state.PlanningSession, operation state.PlanningOperation, reuseExistingProviderSession bool) (string, error) {
	if !reuseExistingProviderSession || store == nil {
		return "", nil
	}
	invocation, found, err := store.GetLatestReusablePlanningProviderInvocation(ctx, planningLogicalSessionID(session.PlanningSessionID, operation.Type))
	if err != nil {
		return "", err
	}
	if !found {
		return "", nil
	}
	return strings.TrimSpace(invocation.ProviderSessionID), nil
}

func (r *operationRunner) buildTerminalEventFromCursor(ctx context.Context, operationID string, cursor *sequenceCursor, eventType string, payload any) (*state.AppendPlanningOperationEventRequest, error) {
	if cursor != nil {
		return r.buildTerminalEvent(operationID, cursor, eventType, payload)
	}
	startSeq, err := r.nextOperationEventSeq(ctx, operationID)
	if err != nil {
		return nil, err
	}
	return r.buildTerminalEvent(operationID, newSequenceCursor(startSeq), eventType, payload)
}

func (r *operationRunner) claimOperation(ctx context.Context, operationID string) (state.PlanningOperation, bool, error) {
	now := nowTime(r.deps.nowString())
	claimed, ok, err := r.deps.store.ClaimOperationWithObserver(ctx, operationID, r.deps.leaseOwner, now.Add(planningOperationLeaseTTL).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), func(stats sqlitex.Stats) {
		if r.deps.metrics != nil && stats.Attempts > 1 {
			r.deps.metrics.claimRetries.Add(uint64(stats.Attempts - 1))
		}
	})
	if err != nil || !ok {
		return claimed, ok, err
	}
	return claimed, true, nil
}

func (r *operationRunner) claimOperationWithRetry(ctx context.Context, operationID string, source operationStartSource) (state.PlanningOperation, bool, error) {
	claimed, claimedOK, err := r.claimOperation(ctx, operationID)
	if err != nil {
		r.observeClaimFailure(source, err)
		return state.PlanningOperation{}, false, err
	}
	if claimedOK {
		r.observeClaimSuccess(source)
		if r.deps.retryWakeProgress != nil {
			r.deps.retryWakeProgress()
		}
	}
	return claimed, claimedOK, nil
}

func (r *operationRunner) observeStartRequested(source operationStartSource) {
	if r == nil || r.deps.metrics == nil {
		return
	}
	switch source {
	case operationStartSourceRecovery:
		r.deps.metrics.recoveryStarts.Add(1)
	default:
		r.deps.metrics.foregroundStarts.Add(1)
	}
}

func (r *operationRunner) observeClaimSuccess(source operationStartSource) {
	if r == nil || r.deps.metrics == nil {
		return
	}
	switch source {
	case operationStartSourceRecovery:
		r.deps.metrics.recoveryClaims.Add(1)
	default:
		r.deps.metrics.foregroundClaims.Add(1)
	}
}

func (r *operationRunner) observeClaimFailure(source operationStartSource, err error) {
	if r == nil || r.deps.metrics == nil || err == nil {
		return
	}
	r.deps.metrics.claimFailures.Add(1)
}

func (r *operationRunner) observeStartFailure(source operationStartSource, err error) {
	if r == nil || r.deps.metrics == nil || err == nil {
		return
	}
	r.deps.metrics.recordStartFailure(r.deps.nowString(), err)
}

func queueAgeMilliseconds(operation state.PlanningOperation) int64 {
	if strings.TrimSpace(operation.CreatedAt) == "" || operation.StartedAt == nil || strings.TrimSpace(*operation.StartedAt) == "" {
		return -1
	}
	createdAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(operation.CreatedAt))
	if err != nil {
		return -1
	}
	startedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(*operation.StartedAt))
	if err != nil || startedAt.Before(createdAt) {
		return -1
	}
	return startedAt.Sub(createdAt).Milliseconds()
}

func sanitizeObservedPlannerOutput(operationType state.PlanningOperationType, outputText string) string {
	trimmed := strings.TrimSpace(outputText)
	if trimmed == "" {
		return ""
	}
	switch operationType {
	case state.PlanningOperationTypeReply, state.PlanningOperationTypeRegenerate:
		// Inline diagnostic observations retain their complete value on a local
		// staging failure. Referenced provider output bypasses this scalar path.
		parsed, err := parsePlannerTerminal(context.Background(), ProviderExecutionMetadata{FinalText: trimmed}, false, nil)
		if err != nil {
			return trimmed
		}
		defer parsed.Close()
		reader, err := parsed.body.Open(context.Background())
		if err != nil {
			return trimmed
		}
		defer reader.Close()
		var cleaned strings.Builder
		if _, err := io.Copy(&cleaned, reader); err != nil {
			return trimmed
		}
		return cleaned.String()
	default:
		return trimmed
	}
}

func (r *operationRunner) startHeartbeat(ctx context.Context, operation state.PlanningOperation, cancelExecution context.CancelFunc) <-chan error {
	result := make(chan error, 1)
	go func() {
		defer close(result)
		tickerFactory := r.deps.newLeaseTicker
		if tickerFactory == nil {
			tickerFactory = newTimePlanningOperationLeaseTicker
		}
		ticker := tickerFactory(planningOperationLeaseTTL / 3)
		if ticker == nil {
			err := fmt.Errorf("planning operation lease ticker is required")
			if cancelExecution != nil {
				cancelExecution()
			}
			result <- err
			return
		}
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				result <- nil
				return
			case <-ticker.C():
				now := nowTime(r.deps.nowString())
				if err := r.deps.store.RenewOperationLease(ctx, operation.OperationID, operation.LeaseOwner, operation.LeaseVersion, now.Add(planningOperationLeaseTTL).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
					if cancelExecution != nil {
						cancelExecution()
					}
					result <- err
					return
				}
			}
		}
	}()
	return result
}

// nilIfEmpty returns a *string for a possibly-empty parent id (nil = root).
func nilIfEmpty(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func planningMessagePayload(message state.PlanningMessage) map[string]any {
	payload := map[string]any{
		"planning_message_id": message.PlanningMessageID,
		"planning_session_id": message.PlanningSessionID,
		"role":                message.Role,
		"content_markdown":    message.ContentMarkdown,
		"created_at":          message.CreatedAt,
	}
	if message.ParentMessageID != nil {
		payload["parent_message_id"] = *message.ParentMessageID
	}
	if question := questionPayloadFromJSON(message.QuestionPayloadJSON); question != nil {
		payload["question_payload"] = structuredQuestionPayload(question)
	}
	if answer := answerPayloadMap(message.AnswerPayloadJSON); len(answer) > 0 {
		payload["answer_payload"] = answer
	}
	return payload
}

func streamingMessagePartPayload(operation state.PlanningOperation, request operationRequest) map[string]any {
	payload := map[string]any{}
	switch operation.Type {
	case state.PlanningOperationTypeReply, state.PlanningOperationTypeRegenerate:
		if trimmed := strings.TrimSpace(request.AssistantMessageID); trimmed != "" {
			payload["planning_message_id"] = trimmed
		}
		if trimmed := strings.TrimSpace(request.ParentMessageID); trimmed != "" {
			payload["parent_message_id"] = trimmed
		}
		payload["role"] = state.PlanningMessageRoleAssistant
		if trimmed := strings.TrimSpace(request.AssistantCreatedAt); trimmed != "" {
			payload["created_at"] = trimmed
		}
	}
	return plannerMessagePayload(operation, payload)
}

func structuredQuestionPayload(question *StructuredQuestion) map[string]any {
	if question == nil {
		return map[string]any{}
	}
	payload := map[string]any{
		"prompt": question.Prompt,
		"type":   question.Type,
	}
	if len(question.Options) > 0 {
		options := make([]map[string]any, 0, len(question.Options))
		for _, option := range question.Options {
			options = append(options, map[string]any{"label": option.Label, "value": option.Value})
		}
		payload["options"] = options
	}
	return payload
}

func sessionPatchPayload(session state.PlanningSession) map[string]any {
	fields := map[string]any{}
	if session.CurrentTitleOperationID != nil {
		if trimmed := strings.TrimSpace(*session.CurrentTitleOperationID); trimmed != "" {
			fields["current_title_run_id"] = acp.PlannerRunID(trimmed)
			fields["auto_title_eligible"] = false
		}
	}
	if trimmed := strings.TrimSpace(session.UpdatedAt); trimmed != "" {
		fields["updated_at"] = trimmed
	}
	return fields
}

func answerPayloadMap(raw string) map[string]any {
	trimmed := firstNonEmpty(raw, `{}`)
	if trimmed == "{}" {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return nil
	}
	return payload
}

func nowTime(now string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, now)
	if err == nil {
		return parsed
	}
	return time.Now().UTC()
}

func requiresFreshProviderSession(ctx context.Context, agent Agent, operationID string) bool {
	if policy, ok := agent.(interface {
		RequiresFreshSessionForOperation(context.Context, string) (bool, error)
	}); ok {
		fresh, err := policy.RequiresFreshSessionForOperation(ctx, operationID)
		return err != nil || fresh
	}
	policy, ok := agent.(interface{ RequiresFreshSession() bool })
	return ok && policy.RequiresFreshSession()
}
