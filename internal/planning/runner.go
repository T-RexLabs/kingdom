package planning

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"kingdom/internal/acp"
	"kingdom/internal/bootstrapassets"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

// terminalCancelWriteTimeout bounds the persistence of a cancel-driven
// terminal row. cancelOperation runs the write under context.WithoutCancel
// so the cancelled service ctx can't abort the row that records the
// cancellation. Without an upper bound, a contended SQLite write could
// outlive the shutdown coordinator's grace window and continue mutating
// the DB after kingdomd believed the runner had drained — racing
// db.Close. This timeout is strictly less than the kingdomd shutdown
// coordinator's 10s grace; SQLite writes here are normally sub-second.
const terminalCancelWriteTimeout = 5 * time.Second

const (
	planningStartMaxClaimAttempts = 4
	planningStartRetryBaseDelay   = 100 * time.Millisecond
)

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
	idle := make(chan struct{})
	close(idle)
	return &operationRunner{
		deps:          deps,
		events:        newOperationEventService(deps),
		ctx:           ctx,
		running:       make(map[string]struct{}),
		cancels:       make(map[string]context.CancelFunc),
		cancelReasons: make(map[string]string),
		idle:          idle,
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
		if err := r.execute(execCtx, operationID, source); err != nil {
			if r.shouldRecordCancelDrainFailure(err) {
				r.recordCancelDrainFailure(operationID, err)
			}
			if !errors.Is(err, context.Canceled) && !errors.Is(err, state.ErrPlanningLeaseConflict) {
				r.observeStartFailure(source, err)
				log.Printf("planner start source=%s operation_id=%s err=%v", source, operationID, err)
			}
		}
	}(opCtx)
}

// shouldRecordCancelDrainFailure narrows the recorded set to genuine
// shutdown-drain failures. Three exclusions:
//   - r.ctx.Err() == nil → not a shutdown-driven path; ordinary errors
//     during normal operation have their own surfaces (failureCount
//     metric, error events, etc.).
//   - context.Canceled → cancelOperation's clean-success sentinel
//     (see its return-value contract); the cancel write succeeded.
//   - state.ErrPlanningLeaseConflict → benign race; another observer
//     already terminalized the row. Recording would falsely mark a
//     clean-but-raced cancel as a shutdown failure.
func (r *operationRunner) shouldRecordCancelDrainFailure(err error) bool {
	if r.ctx == nil || r.ctx.Err() == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, state.ErrPlanningLeaseConflict) {
		return false
	}
	return true
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

	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()
	heartbeatErrCh := r.startHeartbeat(heartbeatCtx, claimed)

	execErr := r.executeClaimed(ctx, claimed, source)
	cancelHeartbeat()
	if heartbeatErr := <-heartbeatErrCh; execErr == nil && heartbeatErr != nil {
		execErr = heartbeatErr
	}
	return execErr
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
	case state.PlanningOperationTypeBootstrapContext:
		return r.executeBootstrapOperation(ctx, session, operation, cursor)
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

func (r *operationRunner) executeBootstrapOperation(ctx context.Context, session state.PlanningSession, operation state.PlanningOperation, cursor *sequenceCursor) error {
	snapshot, err := r.deps.refreshContextSnapshot(ctx, session.PlanningSessionID)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}

	now := r.deps.nowString()
	// Reserve the next event sequence on our cursor; the store builds the
	// terminal event payload internally from the adopted snapshot id so
	// observers never see a candidate id the session ultimately did not
	// adopt. The lease fence inside FinalizeBootstrap means a stale worker
	// gets ErrPlanningLeaseConflict and the snapshot publish never happens.
	seq := cursor.Reserve()
	adoptedSnapshotID, err := r.deps.store.FinalizeBootstrap(ctx, state.FinalizeBootstrapRequest{
		SessionID:                  session.PlanningSessionID,
		OperationID:                operation.OperationID,
		LeaseOwner:                 operation.LeaseOwner,
		LeaseVersion:               operation.LeaseVersion,
		UpdatedAt:                  now,
		FinishedAt:                 now,
		CandidateContextSnapshotID: snapshot.PlanningContextSnapshotID,
		TerminalEventID:            fmt.Sprintf("%s:%d", operation.OperationID, seq),
		TerminalEventSeq:           seq,
		TerminalEventCreatedAt:     now,
	})
	if err != nil {
		// We already reserved a seq; failOperation will reserve another.
		// That's fine — sequence numbers do not need to be densely packed,
		// only monotonic per operation, and gaps after an aborted finalize
		// are normal.
		return r.failOperation(ctx, operation, cursor, err)
	}
	_ = adoptedSnapshotID // adopted id is stored in the terminal event payload + result_ref

	if r.deps.broker != nil {
		r.deps.broker.Publish(operation.OperationID)
	}
	return nil
}

func (r *operationRunner) executeReplyOperation(ctx context.Context, session state.PlanningSession, operation state.PlanningOperation, request operationRequest, cursor *sequenceCursor) error {
	if r.deps.planner == nil {
		return r.failOperation(ctx, operation, cursor, fmt.Errorf("planning agent is not configured"))
	}
	messages, err := r.deps.store.ListMessages(ctx, session.PlanningSessionID)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	snapshot, _, err := r.deps.store.GetLatestContextSnapshot(ctx, session.PlanningSessionID)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	providerInvocation, err := r.createPlanningProviderInvocation(ctx, session, operation, !request.ForceNewSession)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	operation.ActiveProviderInvocationID = stringPtr(providerInvocation.InvocationID)

	streamCtx := decoupleCancellation(ctx)
	r.decorateProviderInvocationContext(streamCtx, operation, cursor, providerInvocation)
	textWriter := newBufferedOperationTextWriter(streamCtx, operation.OperationID, cursor, streamingMessagePartPayload(operation, request), r.appendOperationEvent)
	agentContext := r.deps.agentContext(session, snapshot, messages, request.ForceNewSession)
	agentContext.ProviderInvocation = providerInvocation
	turn, err := r.deps.planner.ContinueTurnStream(ctx, agentContext, func(text string) {
		textWriter.Write(text)
	})
	if flushErr := textWriter.Close(); err == nil && flushErr != nil {
		err = flushErr
	}
	// Shutdown short-circuit: if the service ctx was cancelled while the
	// planner was running, finalize as cancelled regardless of how the
	// planner returned. A cooperative planner returns ctx.Canceled here;
	// an uncooperative one might still return success after shutdown,
	// and writing a `done` terminal event for an operation the user
	// expected to be cancelled would lie. Same pattern repeats after
	// every long planner call below.
	if reason := r.interruptReasonOrNil(operation.OperationID); reason != nil {
		return r.cancelOperation(ctx, operation, cursor, *reason)
	}
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	persistObservedPlanningProviderSession(providerInvocation, turn.Metadata.ProviderSessionID)
	if err := requirePlannerTurnSettled(turn.Metadata); err != nil {
		return r.failOperationWithObservedOutput(ctx, operation, cursor, executionError(turn.Metadata, err), turn.Metadata.FinalText)
	}
	if err := r.reconcileProviderSessionActivity(ctx, operation, cursor, providerInvocation); err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	var autoTitleSession *state.PlanningSession
	autoTitleSession, err = maybeAutoGenerateTitleForReplyFinalize(ctx, r.deps, r, session, messages, request, titleTriggerSourceAutoSecondUserMessageFinalize)
	if err != nil {
		log.Printf("planner auto title source=%s session_id=%s err=%v", titleTriggerSourceAutoSecondUserMessageFinalize, session.PlanningSessionID, err)
		autoTitleSession = nil
	}
	if _, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.GenericEventType, plannerPhasePayload(operation, "finalizing", nil)); err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	questionPayloadJSON, err := marshalQuestion(turn.Question)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}

	assistantMessage := state.AppendPlanningMessageRequest{
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
	if _, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.MessageDoneEventType, messageCompletedPayload); err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	runCompletedPayload := plannerMessagePayload(operation, map[string]any{
		"result_ref":          request.AssistantMessageID,
		"planning_message_id": request.AssistantMessageID,
	})
	if autoTitleSession != nil {
		runCompletedPayload["session_patch"] = sessionPatchPayload(*autoTitleSession)
	}
	terminalEvent, err := r.buildTerminalEvent(operation.OperationID, cursor, acp.RunCompletedEventType, runCompletedPayload)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	now := r.deps.nowString()
	// The user message was appended at create time; finalize appends the
	// assistant under it and moves the active leaf onto the new reply.
	finalizeReq := state.FinalizeOperationRequest{
		Kind:                state.FinalizeReply,
		SessionID:           session.PlanningSessionID,
		OperationID:         operation.OperationID,
		LeaseOwner:          operation.LeaseOwner,
		LeaseVersion:        operation.LeaseVersion,
		ConversationState:   state.PlanningSessionConversationStateReady,
		UpdatedAt:           now,
		FinishedAt:          now,
		AssistantMessage:    &assistantMessage,
		ActiveLeafMessageID: request.AssistantMessageID,
		ResultRef:           request.AssistantMessageID,
		ProviderSettlement:  providerSettlementForSuccess(providerInvocation.InvocationID, turn.Metadata, now),
		TerminalEvent:       terminalEvent,
	}
	if err := r.deps.store.FinalizeOperation(ctx, finalizeReq); err != nil {
		return err
	}
	if r.deps.broker != nil {
		r.deps.broker.Publish(operation.OperationID)
	}
	return nil
}

func (r *operationRunner) executeRegenerateOperation(ctx context.Context, session state.PlanningSession, operation state.PlanningOperation, request operationRequest, cursor *sequenceCursor) error {
	if r.deps.planner == nil {
		return r.failOperation(ctx, operation, cursor, fmt.Errorf("planning agent is not configured"))
	}
	messages, err := r.deps.store.ListMessages(ctx, session.PlanningSessionID)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	snapshot, _, err := r.deps.store.GetLatestContextSnapshot(ctx, session.PlanningSessionID)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	providerInvocation, err := r.createPlanningProviderInvocation(ctx, session, operation, true)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	operation.ActiveProviderInvocationID = stringPtr(providerInvocation.InvocationID)

	streamCtx := decoupleCancellation(ctx)
	r.decorateProviderInvocationContext(streamCtx, operation, cursor, providerInvocation)
	textWriter := newBufferedOperationTextWriter(streamCtx, operation.OperationID, cursor, streamingMessagePartPayload(operation, request), r.appendOperationEvent)
	agentContext := r.deps.agentContext(session, snapshot, messages, request.ForceNewSession)
	agentContext.ProviderInvocation = providerInvocation
	turn, err := r.deps.planner.ContinueTurnStream(ctx, agentContext, func(text string) {
		textWriter.Write(text)
	})
	if flushErr := textWriter.Close(); err == nil && flushErr != nil {
		err = flushErr
	}
	if reason := r.interruptReasonOrNil(operation.OperationID); reason != nil {
		return r.cancelOperation(ctx, operation, cursor, *reason)
	}
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	persistObservedPlanningProviderSession(providerInvocation, turn.Metadata.ProviderSessionID)
	if err := requirePlannerTurnSettled(turn.Metadata); err != nil {
		return r.failOperationWithObservedOutput(ctx, operation, cursor, executionError(turn.Metadata, err), turn.Metadata.FinalText)
	}
	if err := r.reconcileProviderSessionActivity(ctx, operation, cursor, providerInvocation); err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	if _, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.GenericEventType, plannerPhasePayload(operation, "finalizing", nil)); err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	regenQuestionPayloadJSON, err := marshalQuestion(turn.Question)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}

	regenParentID := nilIfEmpty(request.ParentMessageID)
	assistantMessage := state.AppendPlanningMessageRequest{
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
	if _, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.MessageDoneEventType, messageCompletedPayload); err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	terminalEvent, err := r.buildTerminalEvent(operation.OperationID, cursor, acp.RunCompletedEventType, plannerMessagePayload(operation, map[string]any{
		"result_ref":          request.AssistantMessageID,
		"planning_message_id": request.AssistantMessageID,
	}))
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	now := r.deps.nowString()
	if err := r.deps.store.FinalizeOperation(ctx, state.FinalizeOperationRequest{
		Kind:                state.FinalizeRegenerate,
		SessionID:           session.PlanningSessionID,
		OperationID:         operation.OperationID,
		LeaseOwner:          operation.LeaseOwner,
		LeaseVersion:        operation.LeaseVersion,
		ConversationState:   state.PlanningSessionConversationStateReady,
		UpdatedAt:           now,
		FinishedAt:          now,
		AssistantMessage:    &assistantMessage,
		ActiveLeafMessageID: request.AssistantMessageID,
		ResultRef:           request.AssistantMessageID,
		ProviderSettlement:  providerSettlementForSuccess(providerInvocation.InvocationID, turn.Metadata, now),
		TerminalEvent:       terminalEvent,
	}); err != nil {
		return err
	}
	if r.deps.broker != nil {
		r.deps.broker.Publish(operation.OperationID)
	}
	return nil
}

func (r *operationRunner) executeFormalizeOperation(ctx context.Context, session state.PlanningSession, operation state.PlanningOperation, request operationRequest, cursor *sequenceCursor) error {
	if r.deps.planner == nil {
		return r.failOperation(ctx, operation, cursor, fmt.Errorf("planning agent is not configured"))
	}
	messages, err := r.deps.store.ListMessages(ctx, session.PlanningSessionID)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	snapshot, _, err := r.deps.store.GetLatestContextSnapshot(ctx, session.PlanningSessionID)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	providerInvocation, err := r.createPlanningProviderInvocation(ctx, session, operation, true)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	operation.ActiveProviderInvocationID = stringPtr(providerInvocation.InvocationID)
	streamCtx := decoupleCancellation(ctx)
	r.decorateProviderInvocationContext(streamCtx, operation, cursor, providerInvocation)
	textWriter := newBufferedOperationTextWriter(streamCtx, operation.OperationID, cursor, streamingMessagePartPayload(operation, request), r.appendOperationEvent)
	agentContext := r.deps.agentContext(session, snapshot, messages, false)
	agentContext.ProviderInvocation = providerInvocation
	plan, err := r.deps.planner.FormalizePlanStream(ctx, agentContext, func(text string) {
		textWriter.Write(text)
	})
	if flushErr := textWriter.Close(); err == nil && flushErr != nil {
		err = flushErr
	}
	if reason := r.interruptReasonOrNil(operation.OperationID); reason != nil {
		return r.cancelOperation(ctx, operation, cursor, *reason)
	}
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	persistObservedPlanningProviderSession(providerInvocation, plan.Metadata.ProviderSessionID)
	if err := requirePlannerTurnSettled(plan.Metadata); err != nil {
		return r.failOperationWithObservedOutput(ctx, operation, cursor, executionError(plan.Metadata, err), plan.Metadata.FinalText)
	}
	if err := r.reconcileProviderSessionActivity(ctx, operation, cursor, providerInvocation); err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	if _, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.GenericEventType, plannerPhasePayload(operation, "finalizing", nil)); err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	// Drafts no longer carry a hand-crafted summary placeholder; the Decree
	// Plan body itself is the canonical summary. We persist an empty JSON
	// object to satisfy the NOT NULL constraint without lying to the client.
	summaryJSON := []byte(`{}`)
	draft := state.CreatePlanningDraftRequest{
		PlanningDraftID:   request.DraftID,
		PlanningSessionID: session.PlanningSessionID,
		DecreeTitle:       firstNonEmpty(plan.DecreeTitle, session.Title),
		DecreeText:        plan.DecreeText,
		SummaryJSON:       string(summaryJSON),
		CreatedAt:         request.DraftCreatedAt,
	}
	if _, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.MessageDoneEventType, plannerMessagePayload(operation, map[string]any{
		"content_markdown":  draft.DecreeText,
		"text":              draft.DecreeText,
		"decree_title":      draft.DecreeTitle,
		"planning_draft_id": draft.PlanningDraftID,
		"created_at":        draft.CreatedAt,
	})); err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	terminalEvent, err := r.buildTerminalEvent(operation.OperationID, cursor, acp.RunCompletedEventType, plannerMessagePayload(operation, map[string]any{
		"result_ref":        draft.PlanningDraftID,
		"planning_draft_id": draft.PlanningDraftID,
	}))
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	now := r.deps.nowString()
	if err := r.deps.store.FinalizeOperation(ctx, state.FinalizeOperationRequest{
		Kind:               state.FinalizeFormalize,
		SessionID:          session.PlanningSessionID,
		OperationID:        operation.OperationID,
		LeaseOwner:         operation.LeaseOwner,
		LeaseVersion:       operation.LeaseVersion,
		ConversationState:  state.PlanningSessionConversationStateReady,
		UpdatedAt:          now,
		FinishedAt:         now,
		Draft:              &draft,
		CurrentDraftID:     draft.PlanningDraftID,
		ResultRef:          draft.PlanningDraftID,
		ProviderSettlement: providerSettlementForSuccess(providerInvocation.InvocationID, plan.Metadata, now),
		TerminalEvent:      terminalEvent,
	}); err != nil {
		return err
	}
	if r.deps.broker != nil {
		r.deps.broker.Publish(operation.OperationID)
	}
	return nil
}

func (r *operationRunner) executeGenerateTitleOperation(ctx context.Context, session state.PlanningSession, operation state.PlanningOperation, cursor *sequenceCursor) error {
	if r.deps.planner == nil {
		return r.failOperation(ctx, operation, cursor, fmt.Errorf("planning agent is not configured"))
	}
	messages, err := r.deps.store.ListMessages(ctx, session.PlanningSessionID)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	providerInvocation, err := r.createPlanningProviderInvocation(ctx, session, operation, true)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	operation.ActiveProviderInvocationID = stringPtr(providerInvocation.InvocationID)
	streamCtx := decoupleCancellation(ctx)
	r.decorateProviderInvocationContext(streamCtx, operation, cursor, providerInvocation)
	textWriter := newBufferedOperationTextWriter(streamCtx, operation.OperationID, cursor, streamingMessagePartPayload(operation, operationRequest{}), r.appendOperationEvent)
	agentContext := r.deps.titleAgentContext(session, messages)
	agentContext.ProviderInvocation = providerInvocation
	generatedTitle, err := r.deps.planner.GenerateTitle(ctx, agentContext, func(text string) {
		textWriter.Write(text)
	})
	if flushErr := textWriter.Close(); err == nil && flushErr != nil {
		err = flushErr
	}
	if reason := r.interruptReasonOrNil(operation.OperationID); reason != nil {
		return r.cancelOperation(ctx, operation, cursor, *reason)
	}
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	persistObservedPlanningProviderSession(providerInvocation, generatedTitle.Metadata.ProviderSessionID)
	if err := requirePlannerTurnSettled(generatedTitle.Metadata); err != nil {
		return r.failOperationWithObservedOutput(ctx, operation, cursor, executionError(generatedTitle.Metadata, err), generatedTitle.Metadata.FinalText)
	}
	if err := r.reconcileProviderSessionActivity(ctx, operation, cursor, providerInvocation); err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	if _, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.GenericEventType, plannerPhasePayload(operation, "finalizing", nil)); err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	finalTitle := strings.TrimSpace(generatedTitle.Title)
	if finalTitle == "" {
		return r.failOperation(ctx, operation, cursor, fmt.Errorf("generated title is empty"))
	}
	now := r.deps.nowString()
	if _, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.MessageDoneEventType, plannerMessagePayload(operation, map[string]any{
		"content_markdown": finalTitle,
		"text":             finalTitle,
		"title":            finalTitle,
		"title_state":      "generated",
		"created_at":       now,
	})); err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	terminalEvent, err := r.buildTerminalEvent(operation.OperationID, cursor, acp.RunCompletedEventType, plannerMessagePayload(operation, map[string]any{
		"result_ref":  finalTitle,
		"title":       finalTitle,
		"title_state": "generated",
	}))
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	if err := r.deps.store.FinalizeGenerateTitle(ctx, state.FinalizeGenerateTitleRequest{
		SessionID:          session.PlanningSessionID,
		OperationID:        operation.OperationID,
		LeaseOwner:         operation.LeaseOwner,
		LeaseVersion:       operation.LeaseVersion,
		UpdatedAt:          now,
		FinishedAt:         now,
		GeneratedTitle:     finalTitle,
		ObservedOutputText: finalTitle,
		ProviderSettlement: providerSettlementForSuccess(providerInvocation.InvocationID, generatedTitle.Metadata, now),
		TerminalEvent:      terminalEvent,
	}); err != nil {
		return err
	}
	if r.deps.broker != nil {
		r.deps.broker.Publish(operation.OperationID)
	}
	return nil
}

func (r *operationRunner) failOperation(ctx context.Context, operation state.PlanningOperation, cursor *sequenceCursor, cause error) error {
	return r.failOperationWithObservedOutput(ctx, operation, cursor, cause, providerMetadataFromError(cause).FinalText)
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
		return r.cancelOperation(ctx, operation, cursor, *reason)
	}
	if cause == nil {
		cause = fmt.Errorf("planner operation failed")
	}
	errPayload := plannerGenericPayload(operation, acp.KingdomProgressKind, map[string]any{"error": cause.Error()})
	terminalEvent, eventErr := r.buildTerminalEventFromCursor(ctx, operation.OperationID, cursor, acp.RunFailedEventType, errPayload)
	if eventErr != nil {
		return eventErr
	}
	now := r.deps.nowString()
	providerMetadata := providerMetadataFromError(cause)
	observedOutputText = firstNonEmpty(strings.TrimSpace(observedOutputText), strings.TrimSpace(providerMetadata.FinalText))
	observedOutputText = sanitizeObservedPlannerOutput(operation.Type, observedOutputText)
	providerSettlement := providerSettlementForFailure(operation, providerMetadata, now, cause.Error(), cause, false)
	finalizeErr := r.finalizeAbortOperation(ctx, operation, state.PlanningOperationStatusFailed, now, cause.Error(), observedOutputText, providerSettlement, terminalEvent)
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
	return cause
}

// cancelOperation finalizes an in-flight operation as cancelled (status =
// cancelled), distinct from failOperation. Used when the runner observed
// shutdown cancellation on its service-owned context — the operation is
// not a planner failure, just a system-driven interruption, so it must
// not feed the failure metric or land on a UI surface that reads as
// "something went wrong".
//
// The terminal DB write is double-wrapped:
//   - context.WithoutCancel decouples the write from the cancelled service
//     ctx so the cancel-driven shutdown can still persist the cancelled
//     state without aborting itself.
//   - context.WithTimeout(.., terminalCancelWriteTimeout) bounds the write
//     so a contended SQLite lock cannot push DB activity past the
//     shutdown coordinator's grace window.
//
// If the bounded write times out, the row stays `running` and this
// function returns the wrapped DeadlineExceeded. The runner goroutine
// wrapper records that via cancelDrainErr; runner.Wait surfaces it to
// Service.Close which surfaces it to kingdomd's exit code (exitDrainFailure).
// Recovery on the next boot picks up the stuck row via the 3× TTL
// stuck-finalise path. Operators see the failure on the wire instead of
// having to scrape logs.
func (r *operationRunner) cancelOperation(ctx context.Context, operation state.PlanningOperation, cursor *sequenceCursor, reason string) error {
	if strings.TrimSpace(reason) == "" {
		reason = "operation cancelled"
	}
	// Decouple the terminal write from the cancelled service ctx so the
	// cancel-driven shutdown can still persist the cancelled state, AND
	// bound the write so a contended SQLite lock cannot push DB activity
	// past the shutdown coordinator's grace window.
	writeCtx, cancelWrite := context.WithTimeout(context.WithoutCancel(ctx), terminalCancelWriteTimeout)
	defer cancelWrite()
	payload := plannerMessagePayload(operation, map[string]any{"reason": reason})
	terminalEvent, eventErr := r.buildTerminalEventFromCursor(writeCtx, operation.OperationID, cursor, acp.RunCancelledEventType, payload)
	if eventErr != nil {
		return eventErr
	}
	now := r.deps.nowString()
	providerSettlement := providerSettlementForFailure(operation, ProviderExecutionMetadata{}, now, reason, nil, true)
	if err := r.finalizeAbortOperation(writeCtx, operation, state.PlanningOperationStatusCancelled, now, reason, "", providerSettlement, terminalEvent); err != nil {
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

func (r *operationRunner) finalizeAbortOperation(ctx context.Context, operation state.PlanningOperation, status state.PlanningOperationStatus, now, reason, observedOutputText string, providerSettlement *state.FinalizePlanningProviderSettlementRequest, terminalEvent *state.AppendPlanningOperationEventRequest) error {
	switch operation.Type {
	case state.PlanningOperationTypeBootstrapContext:
		return r.deps.store.FinalizeBootstrapAbort(ctx, state.FinalizeBootstrapAbortRequest{
			SessionID:     operation.PlanningSessionID,
			OperationID:   operation.OperationID,
			LeaseOwner:    operation.LeaseOwner,
			LeaseVersion:  operation.LeaseVersion,
			Status:        status,
			UpdatedAt:     now,
			FinishedAt:    now,
			ErrorMessage:  reason,
			TerminalEvent: terminalEvent,
		})
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
			ObservedOutputText: observedOutputText,
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
	if cursor == nil {
		return state.PlanningOperationEvent{}, fmt.Errorf("operation event cursor is required")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return state.PlanningOperationEvent{}, fmt.Errorf("marshal planning operation event payload: %w", err)
	}
	seq := cursor.Reserve()
	event, err := r.deps.store.AppendOperationEvent(ctx, state.AppendPlanningOperationEventRequest{
		EventID:     fmt.Sprintf("%s:%d", operationID, seq),
		OperationID: operationID,
		Seq:         seq,
		EventType:   strings.TrimSpace(eventType),
		PayloadJSON: string(raw),
		CreatedAt:   r.deps.nowString(),
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
	originalProviderSession := invocation.OnProviderSession
	invocation.OnProviderSession = func(providerSessionID string) {
		if originalProviderSession != nil {
			originalProviderSession(providerSessionID)
		}
		_, _ = r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.GenericEventType, plannerPhasePayload(operation, "provider_session_open", map[string]any{"provider_session_id": strings.TrimSpace(providerSessionID)}))
	}
	originalActivity := invocation.OnActivity
	invocation.OnActivity = func(activity ProviderActivity) {
		if originalActivity != nil {
			originalActivity(activity)
		}
		// Prefer the provider part id so a live activity and the same part
		// recovered later by export reconciliation share one stable id; the
		// frontend and reconciliation both de-dup on it. Fall back to a
		// generated id only when the provider omits a part id.
		activityID := firstNonEmpty(strings.TrimSpace(activity.ProviderPartID), strings.TrimSpace(r.deps.nextID("planning_activity")))
		payload := plannerActivityPayload(operation, activityID, activity)
		if len(payload) == 0 {
			return
		}
		event, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.GenericEventType, payload)
		if err == nil {
			publishCanonicalLivePlannerEvent(r.deps.liveBroker, operation.OperationID, event)
		}
	}
}

func plannerActivityPayload(operation state.PlanningOperation, activityID string, activity ProviderActivity) map[string]any {
	occurredAt := strings.TrimSpace(activity.OccurredAt)
	switch strings.TrimSpace(activity.Kind) {
	case "tool":
		toolName := strings.TrimSpace(activity.ToolName)
		if toolName == "" {
			return nil
		}
		payload := plannerGenericPayload(operation, acp.KingdomToolKind, map[string]any{"tool_name": toolName})
		if label := acp.ToolActivityLabel(toolName, activity.ToolStatus, activity.ToolTitle, activity.ToolInput); label != "" {
			payload["label"] = label
		}
		if activityID != "" {
			payload["activity_id"] = activityID
		}
		if occurredAt != "" {
			payload["occurred_at"] = occurredAt
		}
		if trimmedStatus := strings.TrimSpace(activity.ToolStatus); trimmedStatus != "" {
			payload["tool_status"] = trimmedStatus
			if detail := acp.ToolActivityDetail(trimmedStatus); detail != "" {
				payload["detail"] = detail
			}
		}
		if trimmedTitle := strings.TrimSpace(activity.ToolTitle); trimmedTitle != "" {
			payload["tool_title"] = trimmedTitle
		}
		if trimmedInput := strings.TrimSpace(activity.ToolInput); trimmedInput != "" {
			payload["tool_input"] = trimmedInput
		}
		return payload
	case "reasoning":
		payload := plannerGenericPayload(operation, acp.KingdomReasoningKind, map[string]any{"state": "thinking"})
		payload["label"] = "Thinking"
		if activityID != "" {
			payload["activity_id"] = activityID
		}
		if occurredAt != "" {
			payload["occurred_at"] = occurredAt
		}
		if summary := firstNonEmpty(strings.TrimSpace(activity.Summary), strings.TrimSpace(activity.RawText)); summary != "" {
			payload["summary"] = summary
			payload["detail"] = summary
		}
		return payload
	case "progress":
		label := strings.TrimSpace(activity.Label)
		if label == "" {
			return nil
		}
		normalizedLabel, ok := acp.NormalizeProgressActivityLabel(label)
		if !ok {
			return nil
		}
		payload := plannerGenericPayload(operation, acp.KingdomProgressKind, map[string]any{"label": normalizedLabel})
		if activityID != "" {
			payload["activity_id"] = activityID
		}
		if occurredAt != "" {
			payload["occurred_at"] = occurredAt
		}
		return payload
	default:
		return nil
	}
}

func (r *operationRunner) reconcileProviderSessionActivity(ctx context.Context, operation state.PlanningOperation, cursor *sequenceCursor, invocation *ProviderInvocationContext) error {
	if r == nil || r.deps.store == nil || cursor == nil || invocation == nil {
		return nil
	}
	providerSessionID := strings.TrimSpace(invocation.ProviderSessionID)
	invocationID := strings.TrimSpace(invocation.InvocationID)
	if providerSessionID == "" || invocationID == "" || strings.TrimSpace(r.deps.projectRoot) == "" {
		return nil
	}
	items, err := inspectPlannerProviderSessionActivity(ctx, r.deps.projectRoot, operation.OperationID, providerSessionID, invocationID, operation.StartedAt, operation.CreatedAt)
	if err != nil {
		return nil
	}
	if len(items) == 0 {
		return nil
	}
	storedEvents, err := r.deps.store.ListOperationEventsAfterEventIDPage(ctx, operation.OperationID, "", state.DefaultPlanningOperationEventListLimit)
	if err != nil {
		return err
	}
	// Live activity events already carry the provider part id as their
	// `activity_id`, so the durable backstop de-dups primarily on that stable
	// id. The content signature stays as a secondary guard for the rare event
	// the provider emits without a part id.
	seenIDs := make(map[string]struct{}, len(storedEvents))
	seen := make(map[string]struct{}, len(storedEvents))
	for _, event := range storedEvents {
		if activityID, ok := plannerActivityIDFromStoredEvent(event); ok {
			seenIDs[activityID] = struct{}{}
		}
		if signature, ok := plannerActivitySignatureFromStoredEvent(event); ok {
			seen[signature] = struct{}{}
		}
	}
	for _, item := range items {
		activity := providerActivityFromSessionActivityItem(item)
		if activity.Kind == "" {
			continue
		}
		itemID := strings.TrimSpace(item.ID)
		if itemID != "" {
			if _, exists := seenIDs[itemID]; exists {
				continue
			}
		}
		signature := plannerActivitySignature(activity)
		if _, exists := seen[signature]; exists {
			continue
		}
		payload := plannerActivityPayload(operation, itemID, activity)
		if len(payload) == 0 {
			continue
		}
		if _, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, acp.GenericEventType, payload); err != nil {
			return err
		}
		if itemID != "" {
			seenIDs[itemID] = struct{}{}
		}
		seen[signature] = struct{}{}
	}
	return nil
}

func inspectPlannerProviderSessionActivity(ctx context.Context, projectRoot, operationID, providerSessionID, invocationID string, startedAt *string, createdAt string) ([]sessionActivityItem, error) {
	invocationStartedAt := time.Time{}
	if startedAt != nil && strings.TrimSpace(*startedAt) != "" {
		if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(*startedAt)); err == nil {
			invocationStartedAt = parsed.UTC()
		}
	}
	if invocationStartedAt.IsZero() && strings.TrimSpace(createdAt) != "" {
		if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(createdAt)); err == nil {
			invocationStartedAt = parsed.UTC()
		}
	}
	items, err := opencode.InspectSessionActivity(ctx, opencode.SessionActivityRequest{
		ProviderSessionID:   strings.TrimSpace(providerSessionID),
		WorkDir:             strings.TrimSpace(projectRoot),
		ConfigDir:           bootstrapassets.ManagedOpenCodeRoot(projectRoot),
		DatabasePath:        bootstrapassets.ManagedOpenCodeDatabasePath(projectRoot),
		AttemptID:           strings.TrimSpace(operationID),
		InvocationID:        strings.TrimSpace(invocationID),
		InvocationStartedAt: invocationStartedAt,
	})
	if err != nil {
		return nil, err
	}
	result := make([]sessionActivityItem, 0, len(items))
	for _, item := range items {
		result = append(result, sessionActivityItem{
			ID:         strings.TrimSpace(item.ID),
			Kind:       strings.TrimSpace(item.Kind),
			Label:      strings.TrimSpace(item.Label),
			Detail:     strings.TrimSpace(item.Detail),
			Input:      strings.TrimSpace(item.Input),
			Output:     strings.TrimSpace(item.Output),
			OccurredAt: strings.TrimSpace(item.OccurredAt),
		})
	}
	return result, nil
}

type sessionActivityItem struct {
	ID         string
	Kind       string
	Label      string
	Detail     string
	Input      string
	Output     string
	OccurredAt string
}

func providerActivityFromSessionActivityItem(item sessionActivityItem) ProviderActivity {
	activity := ProviderActivity{
		OccurredAt: strings.TrimSpace(item.OccurredAt),
	}
	switch strings.TrimSpace(item.Kind) {
	case "thinking":
		activity.Kind = "reasoning"
		activity.Summary = strings.TrimSpace(item.Detail)
	case "tool":
		activity.Kind = "tool"
		activity.ToolInput = strings.TrimSpace(item.Input)
		activity.ToolName, activity.ToolStatus, activity.ToolTitle = inferToolActivityFields(item)
	case "progress":
		activity.Kind = "progress"
		activity.Label = strings.TrimSpace(item.Label)
	}
	return activity
}

func inferToolActivityFields(item sessionActivityItem) (string, string, string) {
	label := strings.TrimSpace(item.Label)
	input := strings.TrimSpace(item.Input)
	if strings.HasPrefix(label, "Fetched ") {
		return "webfetch", "completed", strings.TrimSpace(strings.TrimPrefix(label, "Fetched "))
	}
	if strings.HasPrefix(label, "Fetching ") {
		return "webfetch", "running", strings.TrimSpace(strings.TrimPrefix(label, "Fetching "))
	}
	if strings.Contains(label, "webfetch failed") {
		return "webfetch", "failed", strings.TrimSpace(strings.TrimPrefix(label, "webfetch failed for "))
	}
	if input != "" {
		return "webfetch", "completed", ""
	}
	return "", "", ""
}

func plannerActivitySignature(activity ProviderActivity) string {
	return strings.Join([]string{
		strings.TrimSpace(activity.Kind),
		strings.TrimSpace(activity.ToolName),
		strings.TrimSpace(activity.ToolStatus),
		strings.TrimSpace(activity.ToolTitle),
		strings.TrimSpace(activity.ToolInput),
		strings.TrimSpace(activity.Label),
		strings.TrimSpace(activity.Summary),
		strings.TrimSpace(activity.OccurredAt),
	}, "\x00")
}

func plannerActivityIDFromStoredEvent(event state.PlanningOperationEvent) (string, bool) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return "", false
	}
	activityID := strings.TrimSpace(plannerStringValue(payload["activity_id"]))
	if activityID == "" {
		return "", false
	}
	return activityID, true
}

func plannerActivitySignatureFromStoredEvent(event state.PlanningOperationEvent) (string, bool) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return "", false
	}
	if strings.TrimSpace(plannerStringValue(payload["kind"])) == "" {
		return "", false
	}
	activity := ProviderActivity{OccurredAt: strings.TrimSpace(plannerStringValue(payload["occurred_at"]))}
	switch strings.TrimSpace(plannerStringValue(payload["kind"])) {
	case acp.KingdomToolKind:
		activity.Kind = "tool"
		activity.ToolName = strings.TrimSpace(plannerStringValue(payload["tool_name"]))
		activity.ToolStatus = strings.TrimSpace(plannerStringValue(payload["tool_status"]))
		activity.ToolTitle = strings.TrimSpace(plannerStringValue(payload["tool_title"]))
		activity.ToolInput = strings.TrimSpace(plannerStringValue(payload["tool_input"]))
	case acp.KingdomReasoningKind:
		activity.Kind = "reasoning"
		activity.Summary = firstNonEmpty(strings.TrimSpace(plannerStringValue(payload["detail"])), strings.TrimSpace(plannerStringValue(payload["summary"])))
	case acp.KingdomProgressKind:
		activity.Kind = "progress"
		activity.Label = strings.TrimSpace(plannerStringValue(payload["label"]))
	default:
		return "", false
	}
	return plannerActivitySignature(activity), true
}

func plannerStringValue(value any) string {
	if typed, ok := value.(string); ok {
		return typed
	}
	return ""
}

func persistObservedPlanningProviderSession(invocation *ProviderInvocationContext, providerSessionID string) {
	if invocation == nil {
		return
	}
	trimmedProviderSessionID := strings.TrimSpace(providerSessionID)
	if trimmedProviderSessionID == "" || strings.TrimSpace(invocation.ProviderSessionID) == trimmedProviderSessionID {
		return
	}
	invocation.ProviderSessionID = trimmedProviderSessionID
}

func publishCanonicalLivePlannerEvent(broker *liveOperationEventBroker, operationID string, event state.PlanningOperationEvent) {
	if broker == nil || strings.TrimSpace(operationID) == "" {
		return
	}
	if strings.TrimSpace(event.EventID) == "" || strings.TrimSpace(event.EventType) == "" || strings.TrimSpace(event.PayloadJSON) == "" {
		return
	}
	broker.Publish(operationID, LiveOperationEvent{
		EventID:   strings.TrimSpace(event.EventID),
		EventType: strings.TrimSpace(event.EventType),
		Payload:   strings.TrimSpace(event.PayloadJSON),
	})
}

// buildTerminalEventFromCursor uses the supplied cursor when available
// (preserving monotonicity with concurrent stream events) and falls back to
// reading the next sequence from storage only when no cursor is in scope —
// e.g. the recovery / stuck-operation paths that finalise without ever
// streaming.
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
	claimed, ok, err := r.deps.store.ClaimOperation(ctx, operationID, r.deps.leaseOwner, now.Add(planningOperationLeaseTTL).Format(time.RFC3339), now.Format(time.RFC3339))
	if err != nil || !ok {
		return claimed, ok, err
	}
	return claimed, true, nil
}

func (r *operationRunner) claimOperationWithRetry(ctx context.Context, operationID string, source operationStartSource) (state.PlanningOperation, bool, error) {
	var lastErr error
	for attempt := 1; attempt <= planningStartMaxClaimAttempts; attempt++ {
		claimed, ok, err := r.claimOperation(ctx, operationID)
		if err == nil {
			if ok {
				r.observeClaimSuccess(source)
			}
			return claimed, ok, nil
		}
		lastErr = err
		r.observeClaimFailure(source, err)
		if ctx.Err() != nil || attempt == planningStartMaxClaimAttempts {
			break
		}
		r.observeClaimRetry(source)
		delay := time.Duration(attempt) * planningStartRetryBaseDelay
		log.Printf("planner claim retry source=%s operation_id=%s attempt=%d delay_ms=%d err=%v", source, operationID, attempt, delay.Milliseconds(), err)
		if sleepErr := sleepWithContext(ctx, delay); sleepErr != nil {
			return state.PlanningOperation{}, false, sleepErr
		}
	}
	if lastErr != nil {
		return state.PlanningOperation{}, false, lastErr
	}
	return state.PlanningOperation{}, false, nil
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

func (r *operationRunner) observeClaimRetry(source operationStartSource) {
	if r == nil || r.deps.metrics == nil {
		return
	}
	r.deps.metrics.claimRetries.Add(1)
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
	createdAt, err := time.Parse(time.RFC3339, strings.TrimSpace(operation.CreatedAt))
	if err != nil {
		return -1
	}
	startedAt, err := time.Parse(time.RFC3339, strings.TrimSpace(*operation.StartedAt))
	if err != nil || startedAt.Before(createdAt) {
		return -1
	}
	return startedAt.Sub(createdAt).Milliseconds()
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func sanitizeObservedPlannerOutput(operationType state.PlanningOperationType, outputText string) string {
	trimmed := strings.TrimSpace(outputText)
	if trimmed == "" {
		return ""
	}
	switch operationType {
	case state.PlanningOperationTypeReply, state.PlanningOperationTypeRegenerate:
		cleaned, _ := parseStructuredQuestion(trimmed)
		return cleaned
	default:
		return trimmed
	}
}

func (r *operationRunner) startHeartbeat(ctx context.Context, operation state.PlanningOperation) <-chan error {
	result := make(chan error, 1)
	go func() {
		defer close(result)
		ticker := time.NewTicker(planningOperationLeaseTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				result <- nil
				return
			case <-ticker.C:
				now := nowTime(r.deps.nowString())
				if err := r.deps.store.RenewOperationLease(ctx, operation.OperationID, operation.LeaseOwner, operation.LeaseVersion, now.Add(planningOperationLeaseTTL).Format(time.RFC3339), now.Format(time.RFC3339)); err != nil {
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
	parsed, err := time.Parse(time.RFC3339, now)
	if err == nil {
		return parsed
	}
	return time.Now().UTC()
}

// decoupleCancellation returns a context that carries the parent's values but
// is not cancelled when the parent is. The streaming agent's onChunk callback
// uses it so that mid-stream HTTP cancellations do not silently drop the next
// few SSE events the runner still needs to emit.
func decoupleCancellation(parent context.Context) context.Context {
	return context.WithoutCancel(parent)
}
