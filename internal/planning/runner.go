package planning

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

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

type operationRunner struct {
	deps   runtimeDeps
	events *operationEventService
	// ctx is the service-owned context — cancelled by Service.Close so
	// every in-flight execute() observes Done at the next chunk boundary
	// and exits, allowing Wait() to drain. Sharing it with the recovery
	// loop means a single recoveryCancel terminates both shutdown paths.
	ctx context.Context

	mu      sync.Mutex
	running map[string]struct{}
	idle    chan struct{}
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
		deps:    deps,
		events:  newOperationEventService(deps),
		ctx:     ctx,
		running: make(map[string]struct{}),
		idle:    idle,
	}
}

func (r *operationRunner) Start(operationID string) {
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
	r.running[operationID] = struct{}{}
	r.wg.Add(1)
	r.mu.Unlock()

	go func() {
		defer r.wg.Done()
		defer r.finish(operationID)
		if err := r.execute(r.ctx, operationID); err != nil && r.shouldRecordCancelDrainFailure(err) {
			r.recordCancelDrainFailure(operationID, err)
		}
	}()
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
	if len(r.running) == 0 && r.idle != nil {
		close(r.idle)
	}
}

func (r *operationRunner) execute(ctx context.Context, operationID string) error {
	claimed, ok, err := r.claimOperation(ctx, operationID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()
	heartbeatErrCh := r.startHeartbeat(heartbeatCtx, claimed)

	execErr := r.executeClaimed(ctx, claimed)
	cancelHeartbeat()
	if heartbeatErr := <-heartbeatErrCh; execErr == nil && heartbeatErr != nil {
		execErr = heartbeatErr
	}
	return execErr
}

func (r *operationRunner) executeClaimed(ctx context.Context, operation state.PlanningOperation) error {
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

	if _, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, "started", map[string]any{"status": "running", "operation_type": operation.Type}); err != nil {
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
	if _, err := r.appendOperationEvent(ctx, operation.OperationID, cursor, "user_message", planningMessagePayload(state.PlanningMessage{
		PlanningMessageID:   request.UserMessageID,
		PlanningSessionID:   session.PlanningSessionID,
		TurnIndex:           request.TurnIndex,
		Role:                state.PlanningMessageRoleUser,
		ContentMarkdown:     request.ContentMarkdown,
		QuestionPayloadJSON: `{}`,
		AnswerPayloadJSON:   firstNonEmpty(string(request.AnswerPayload), `{}`),
		CreatedAt:           request.UserCreatedAt,
	})); err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	providerInvocation, err := r.createPlanningProviderInvocation(ctx, session, operation)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	operation.ActiveProviderInvocationID = stringPtr(providerInvocation.InvocationID)

	streamCtx := decoupleCancellation(ctx)
	agentContext := r.deps.agentContext(session, snapshot, messages, request.ForceNewSession)
	agentContext.ProviderInvocation = providerInvocation
	turn, err := r.deps.planner.ContinueTurnStream(ctx, agentContext, func(text string) {
		_, _ = r.appendOperationEvent(streamCtx, operation.OperationID, cursor, "text_delta", map[string]any{"text": text})
	})
	// Shutdown short-circuit: if the service ctx was cancelled while the
	// planner was running, finalize as cancelled regardless of how the
	// planner returned. A cooperative planner returns ctx.Canceled here;
	// an uncooperative one might still return success after shutdown,
	// and writing a `done` terminal event for an operation the user
	// expected to be cancelled would lie. Same pattern repeats after
	// every long planner call below.
	if reason := r.shutdownReasonOrNil(); reason != nil {
		return r.cancelOperation(ctx, operation, cursor, *reason)
	}
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	if err := requireReceiptEvidenceID(turn.Metadata); err != nil {
		return r.failOperationWithObservedOutput(ctx, operation, cursor, executionError(turn.Metadata, err), turn.Metadata.FinalText)
	}
	questionPayloadJSON, err := marshalQuestion(turn.Question)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}

	assistantMessage := state.AppendPlanningMessageRequest{
		PlanningMessageID:   request.AssistantMessageID,
		PlanningSessionID:   session.PlanningSessionID,
		TurnIndex:           request.TurnIndex,
		Role:                state.PlanningMessageRoleAssistant,
		ContentMarkdown:     turn.ContentMarkdown,
		QuestionPayloadJSON: questionPayloadJSON,
		AnswerPayloadJSON:   `{}`,
		Active:              true,
		CreatedAt:           request.AssistantCreatedAt,
	}
	donePayload := planningMessagePayload(state.PlanningMessage{
		PlanningMessageID:   assistantMessage.PlanningMessageID,
		PlanningSessionID:   assistantMessage.PlanningSessionID,
		TurnIndex:           assistantMessage.TurnIndex,
		Role:                assistantMessage.Role,
		ContentMarkdown:     assistantMessage.ContentMarkdown,
		QuestionPayloadJSON: assistantMessage.QuestionPayloadJSON,
		AnswerPayloadJSON:   assistantMessage.AnswerPayloadJSON,
		VariantIndex:        0,
		Active:              true,
		CreatedAt:           assistantMessage.CreatedAt,
	})
	donePayload["variant_count"] = 1
	terminalEvent, err := r.buildTerminalEvent(operation.OperationID, cursor, "done", donePayload)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	now := r.deps.nowString()
	finalizeReq := state.FinalizeOperationRequest{
		Kind:                     state.FinalizeReply,
		SessionID:                session.PlanningSessionID,
		OperationID:              operation.OperationID,
		LeaseOwner:               operation.LeaseOwner,
		LeaseVersion:             operation.LeaseVersion,
		ConversationState:        state.PlanningSessionConversationStateReady,
		UpdatedAt:                now,
		FinishedAt:               now,
		AssistantMessage:         &assistantMessage,
		CurrentQuestionMessageID: request.AssistantMessageID,
		ResultRef:                request.AssistantMessageID,
		ProviderSettlement:       providerSettlementForSuccess(providerInvocation.InvocationID, turn.Metadata, now),
		TerminalEvent:            terminalEvent,
	}
	if request.ForceNewSession {
		finalizeReq.UserMessage = &state.AppendPlanningMessageRequest{
			PlanningMessageID:   request.UserMessageID,
			PlanningSessionID:   session.PlanningSessionID,
			TurnIndex:           request.TurnIndex,
			Role:                state.PlanningMessageRoleUser,
			ContentMarkdown:     request.ContentMarkdown,
			QuestionPayloadJSON: `{}`,
			AnswerPayloadJSON:   firstNonEmpty(string(request.AnswerPayload), `{}`),
			Active:              true,
			CreatedAt:           request.UserCreatedAt,
		}
		finalizeReq.DeleteMessagesFromTurn = &request.TurnIndex
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
	providerInvocation, err := r.createPlanningProviderInvocation(ctx, session, operation)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	operation.ActiveProviderInvocationID = stringPtr(providerInvocation.InvocationID)

	streamCtx := decoupleCancellation(ctx)
	agentContext := r.deps.agentContext(session, snapshot, messages, request.ForceNewSession)
	agentContext.ProviderInvocation = providerInvocation
	turn, err := r.deps.planner.ContinueTurnStream(ctx, agentContext, func(text string) {
		_, _ = r.appendOperationEvent(streamCtx, operation.OperationID, cursor, "text_delta", map[string]any{"text": text})
	})
	if reason := r.shutdownReasonOrNil(); reason != nil {
		return r.cancelOperation(ctx, operation, cursor, *reason)
	}
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	if err := requireReceiptEvidenceID(turn.Metadata); err != nil {
		return r.failOperationWithObservedOutput(ctx, operation, cursor, executionError(turn.Metadata, err), turn.Metadata.FinalText)
	}
	regenQuestionPayloadJSON, err := marshalQuestion(turn.Question)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}

	assistantMessage := state.AppendPlanningMessageRequest{
		PlanningMessageID:   request.AssistantMessageID,
		PlanningSessionID:   session.PlanningSessionID,
		TurnIndex:           request.TargetTurnIndex,
		Role:                state.PlanningMessageRoleAssistant,
		ContentMarkdown:     turn.ContentMarkdown,
		QuestionPayloadJSON: regenQuestionPayloadJSON,
		AnswerPayloadJSON:   `{}`,
		VariantIndex:        request.VariantIndex,
		Active:              true,
		CreatedAt:           request.AssistantCreatedAt,
	}
	donePayload := planningMessagePayload(state.PlanningMessage{
		PlanningMessageID:   assistantMessage.PlanningMessageID,
		PlanningSessionID:   assistantMessage.PlanningSessionID,
		TurnIndex:           assistantMessage.TurnIndex,
		Role:                assistantMessage.Role,
		ContentMarkdown:     assistantMessage.ContentMarkdown,
		QuestionPayloadJSON: assistantMessage.QuestionPayloadJSON,
		AnswerPayloadJSON:   assistantMessage.AnswerPayloadJSON,
		VariantIndex:        assistantMessage.VariantIndex,
		Active:              assistantMessage.Active,
		CreatedAt:           assistantMessage.CreatedAt,
	})
	donePayload["variant_count"] = request.VariantIndex + 1
	terminalEvent, err := r.buildTerminalEvent(operation.OperationID, cursor, "done", donePayload)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	now := r.deps.nowString()
	if err := r.deps.store.FinalizeOperation(ctx, state.FinalizeOperationRequest{
		Kind:                     state.FinalizeRegenerate,
		SessionID:                session.PlanningSessionID,
		OperationID:              operation.OperationID,
		LeaseOwner:               operation.LeaseOwner,
		LeaseVersion:             operation.LeaseVersion,
		ConversationState:        state.PlanningSessionConversationStateReady,
		UpdatedAt:                now,
		FinishedAt:               now,
		AssistantMessage:         &assistantMessage,
		CurrentQuestionMessageID: request.AssistantMessageID,
		DeleteMessagesAfterTurn:  &request.TargetTurnIndex,
		DeactivateVariantsAtTurn: &request.TargetTurnIndex,
		ResultRef:                request.AssistantMessageID,
		ProviderSettlement:       providerSettlementForSuccess(providerInvocation.InvocationID, turn.Metadata, now),
		TerminalEvent:            terminalEvent,
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
	providerInvocation, err := r.createPlanningProviderInvocation(ctx, session, operation)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	operation.ActiveProviderInvocationID = stringPtr(providerInvocation.InvocationID)
	streamCtx := decoupleCancellation(ctx)
	agentContext := r.deps.agentContext(session, snapshot, messages, false)
	agentContext.ProviderInvocation = providerInvocation
	plan, err := r.deps.planner.FormalizePlanStream(ctx, agentContext, func(text string) {
		_, _ = r.appendOperationEvent(streamCtx, operation.OperationID, cursor, "text_delta", map[string]any{"text": text})
	})
	if reason := r.shutdownReasonOrNil(); reason != nil {
		return r.cancelOperation(ctx, operation, cursor, *reason)
	}
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	if err := requireReceiptEvidenceID(plan.Metadata); err != nil {
		return r.failOperationWithObservedOutput(ctx, operation, cursor, executionError(plan.Metadata, err), plan.Metadata.FinalText)
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
	terminalEvent, err := r.buildTerminalEvent(operation.OperationID, cursor, "done", map[string]any{
		"planning_draft_id": draft.PlanningDraftID,
	})
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
	snapshot, _, err := r.deps.store.GetLatestContextSnapshot(ctx, session.PlanningSessionID)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	providerInvocation, err := r.createPlanningProviderInvocation(ctx, session, operation)
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	operation.ActiveProviderInvocationID = stringPtr(providerInvocation.InvocationID)
	streamCtx := decoupleCancellation(ctx)
	agentContext := r.deps.agentContext(session, snapshot, messages, false)
	agentContext.ProviderInvocation = providerInvocation
	generatedTitle, err := r.deps.planner.GenerateTitle(ctx, agentContext)
	if reason := r.shutdownReasonOrNil(); reason != nil {
		return r.cancelOperation(ctx, operation, cursor, *reason)
	}
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	if err := requireReceiptEvidenceID(generatedTitle.Metadata); err != nil {
		return r.failOperationWithObservedOutput(ctx, operation, cursor, executionError(generatedTitle.Metadata, err), generatedTitle.Metadata.FinalText)
	}
	finalTitle := strings.TrimSpace(generatedTitle.Title)
	if finalTitle == "" {
		return r.failOperation(ctx, operation, cursor, fmt.Errorf("generated title is empty"))
	}
	_, _ = r.appendOperationEvent(streamCtx, operation.OperationID, cursor, "text_delta", map[string]any{"text": finalTitle})
	terminalEvent, err := r.buildTerminalEvent(operation.OperationID, cursor, "done", map[string]any{
		"title":       finalTitle,
		"title_state": "generated",
	})
	if err != nil {
		return r.failOperation(ctx, operation, cursor, err)
	}
	now := r.deps.nowString()
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

func (r *operationRunner) finalizeRecoveredSuccessFromReceipt(ctx context.Context, operation state.PlanningOperation, metadata ProviderExecutionMetadata, now string) error {
	if r == nil || r.deps.store == nil {
		return fmt.Errorf("planning store is required for recovered success finalization")
	}
	if operation.ActiveProviderInvocationID == nil || strings.TrimSpace(*operation.ActiveProviderInvocationID) == "" {
		return fmt.Errorf("recovered success finalization requires an active provider invocation")
	}
	if now = strings.TrimSpace(now); now == "" {
		now = r.deps.nowString()
	}
	request, err := decodeOperationRequest(operation.RequestPayloadJSON)
	if err != nil {
		return err
	}
	session, found, err := r.deps.store.GetSession(ctx, operation.PlanningSessionID)
	if err != nil {
		return err
	}
	if !found {
		return ErrSessionNotFound
	}
	if operation.Type == state.PlanningOperationTypeGenerateTitle {
		finalizeReq, err := r.buildRecoveredGenerateTitleFinalizeRequest(ctx, session, operation, metadata, now)
		if err != nil {
			return err
		}
		if err := r.deps.store.FinalizeGenerateTitle(ctx, finalizeReq); err != nil {
			if errors.Is(err, state.ErrPlanningLeaseConflict) {
				return nil
			}
			return err
		}
		if r.deps.broker != nil {
			r.deps.broker.Publish(operation.OperationID)
		}
		return nil
	}
	finalizeReq, err := r.buildRecoveredSuccessFinalizeRequest(ctx, session, operation, request, metadata, now)
	if err != nil {
		return err
	}
	if err := r.deps.store.FinalizeOperation(ctx, finalizeReq); err != nil {
		if errors.Is(err, state.ErrPlanningLeaseConflict) {
			return nil
		}
		return err
	}
	if r.deps.broker != nil {
		r.deps.broker.Publish(operation.OperationID)
	}
	return nil
}

func (r *operationRunner) buildRecoveredSuccessFinalizeRequest(ctx context.Context, session state.PlanningSession, operation state.PlanningOperation, request operationRequest, metadata ProviderExecutionMetadata, now string) (state.FinalizeOperationRequest, error) {
	base := state.FinalizeOperationRequest{
		SessionID:          session.PlanningSessionID,
		OperationID:        operation.OperationID,
		LeaseOwner:         operation.LeaseOwner,
		LeaseVersion:       operation.LeaseVersion,
		ConversationState:  state.PlanningSessionConversationStateReady,
		UpdatedAt:          now,
		FinishedAt:         now,
		ProviderSettlement: providerSettlementForSuccess(*operation.ActiveProviderInvocationID, metadata, now),
	}
	if base.ProviderSettlement == nil {
		return state.FinalizeOperationRequest{}, fmt.Errorf("recovered success finalization requires completion receipt settlement")
	}

	switch operation.Type {
	case state.PlanningOperationTypeReply:
		assistantMessageID := strings.TrimSpace(request.AssistantMessageID)
		if assistantMessageID == "" {
			return state.FinalizeOperationRequest{}, fmt.Errorf("reply recovery requires assistant_message_id in request payload")
		}
		cleanMarkdown, question := parseStructuredQuestion(metadata.FinalText)
		questionPayloadJSON, err := marshalQuestion(question)
		if err != nil {
			return state.FinalizeOperationRequest{}, err
		}
		assistantMessage := state.AppendPlanningMessageRequest{
			PlanningMessageID:   assistantMessageID,
			PlanningSessionID:   session.PlanningSessionID,
			TurnIndex:           request.TurnIndex,
			Role:                state.PlanningMessageRoleAssistant,
			ContentMarkdown:     cleanMarkdown,
			QuestionPayloadJSON: questionPayloadJSON,
			AnswerPayloadJSON:   `{}`,
			Active:              true,
			CreatedAt:           firstNonEmpty(strings.TrimSpace(request.AssistantCreatedAt), now),
		}
		donePayload := planningMessagePayload(state.PlanningMessage{
			PlanningMessageID:   assistantMessage.PlanningMessageID,
			PlanningSessionID:   assistantMessage.PlanningSessionID,
			TurnIndex:           assistantMessage.TurnIndex,
			Role:                assistantMessage.Role,
			ContentMarkdown:     assistantMessage.ContentMarkdown,
			QuestionPayloadJSON: assistantMessage.QuestionPayloadJSON,
			AnswerPayloadJSON:   assistantMessage.AnswerPayloadJSON,
			VariantIndex:        0,
			Active:              true,
			CreatedAt:           assistantMessage.CreatedAt,
		})
		donePayload["variant_count"] = 1
		terminalEvent, err := r.buildTerminalEventFromCursor(ctx, operation.OperationID, nil, "done", donePayload)
		if err != nil {
			return state.FinalizeOperationRequest{}, err
		}
		base.Kind = state.FinalizeReply
		base.AssistantMessage = &assistantMessage
		base.CurrentQuestionMessageID = assistantMessage.PlanningMessageID
		base.ResultRef = assistantMessage.PlanningMessageID
		base.TerminalEvent = terminalEvent
		if request.ForceNewSession {
			userMessageID := strings.TrimSpace(request.UserMessageID)
			if userMessageID == "" {
				return state.FinalizeOperationRequest{}, fmt.Errorf("reply recovery requires user_message_id when force_new_session is set")
			}
			base.UserMessage = &state.AppendPlanningMessageRequest{
				PlanningMessageID:   userMessageID,
				PlanningSessionID:   session.PlanningSessionID,
				TurnIndex:           request.TurnIndex,
				Role:                state.PlanningMessageRoleUser,
				ContentMarkdown:     request.ContentMarkdown,
				QuestionPayloadJSON: `{}`,
				AnswerPayloadJSON:   firstNonEmpty(string(request.AnswerPayload), `{}`),
				Active:              true,
				CreatedAt:           firstNonEmpty(strings.TrimSpace(request.UserCreatedAt), now),
			}
			base.DeleteMessagesFromTurn = &request.TurnIndex
		}
		return base, nil
	case state.PlanningOperationTypeRegenerate:
		assistantMessageID := strings.TrimSpace(request.AssistantMessageID)
		if assistantMessageID == "" {
			return state.FinalizeOperationRequest{}, fmt.Errorf("regenerate recovery requires assistant_message_id in request payload")
		}
		cleanMarkdown, question := parseStructuredQuestion(metadata.FinalText)
		questionPayloadJSON, err := marshalQuestion(question)
		if err != nil {
			return state.FinalizeOperationRequest{}, err
		}
		assistantMessage := state.AppendPlanningMessageRequest{
			PlanningMessageID:   assistantMessageID,
			PlanningSessionID:   session.PlanningSessionID,
			TurnIndex:           request.TargetTurnIndex,
			Role:                state.PlanningMessageRoleAssistant,
			ContentMarkdown:     cleanMarkdown,
			QuestionPayloadJSON: questionPayloadJSON,
			AnswerPayloadJSON:   `{}`,
			VariantIndex:        request.VariantIndex,
			Active:              true,
			CreatedAt:           firstNonEmpty(strings.TrimSpace(request.AssistantCreatedAt), now),
		}
		donePayload := planningMessagePayload(state.PlanningMessage{
			PlanningMessageID:   assistantMessage.PlanningMessageID,
			PlanningSessionID:   assistantMessage.PlanningSessionID,
			TurnIndex:           assistantMessage.TurnIndex,
			Role:                assistantMessage.Role,
			ContentMarkdown:     assistantMessage.ContentMarkdown,
			QuestionPayloadJSON: assistantMessage.QuestionPayloadJSON,
			AnswerPayloadJSON:   assistantMessage.AnswerPayloadJSON,
			VariantIndex:        assistantMessage.VariantIndex,
			Active:              assistantMessage.Active,
			CreatedAt:           assistantMessage.CreatedAt,
		})
		donePayload["variant_count"] = request.VariantIndex + 1
		terminalEvent, err := r.buildTerminalEventFromCursor(ctx, operation.OperationID, nil, "done", donePayload)
		if err != nil {
			return state.FinalizeOperationRequest{}, err
		}
		base.Kind = state.FinalizeRegenerate
		base.AssistantMessage = &assistantMessage
		base.CurrentQuestionMessageID = assistantMessage.PlanningMessageID
		base.DeleteMessagesAfterTurn = &request.TargetTurnIndex
		base.DeactivateVariantsAtTurn = &request.TargetTurnIndex
		base.ResultRef = assistantMessage.PlanningMessageID
		base.TerminalEvent = terminalEvent
		return base, nil
	case state.PlanningOperationTypeFormalize:
		draftID := strings.TrimSpace(request.DraftID)
		if draftID == "" {
			return state.FinalizeOperationRequest{}, fmt.Errorf("formalize recovery requires draft_id in request payload")
		}
		title, text := parseFormalizedOutput(metadata.FinalText, session.Title)
		draft := state.CreatePlanningDraftRequest{
			PlanningDraftID:   draftID,
			PlanningSessionID: session.PlanningSessionID,
			DecreeTitle:       firstNonEmpty(title, session.Title),
			DecreeText:        text,
			SummaryJSON:       `{}`,
			CreatedAt:         firstNonEmpty(strings.TrimSpace(request.DraftCreatedAt), now),
		}
		terminalEvent, err := r.buildTerminalEventFromCursor(ctx, operation.OperationID, nil, "done", map[string]any{"planning_draft_id": draft.PlanningDraftID})
		if err != nil {
			return state.FinalizeOperationRequest{}, err
		}
		base.Kind = state.FinalizeFormalize
		base.Draft = &draft
		base.CurrentDraftID = draft.PlanningDraftID
		base.ResultRef = draft.PlanningDraftID
		base.TerminalEvent = terminalEvent
		return base, nil
	default:
		return state.FinalizeOperationRequest{}, fmt.Errorf("recovered success finalization is unsupported for operation type %q", operation.Type)
	}
}

func (r *operationRunner) buildRecoveredGenerateTitleFinalizeRequest(ctx context.Context, session state.PlanningSession, operation state.PlanningOperation, metadata ProviderExecutionMetadata, now string) (state.FinalizeGenerateTitleRequest, error) {
	finalTitle, err := normalizeGeneratedTitle(metadata.FinalText)
	if err != nil {
		return state.FinalizeGenerateTitleRequest{}, &invalidRecoveredGeneratedTitleError{cause: err}
	}
	terminalEvent, err := r.buildTerminalEventFromCursor(ctx, operation.OperationID, nil, "done", map[string]any{
		"title":       finalTitle,
		"title_state": "generated",
	})
	if err != nil {
		return state.FinalizeGenerateTitleRequest{}, err
	}
	return state.FinalizeGenerateTitleRequest{
		SessionID:          session.PlanningSessionID,
		OperationID:        operation.OperationID,
		LeaseOwner:         operation.LeaseOwner,
		LeaseVersion:       operation.LeaseVersion,
		UpdatedAt:          now,
		FinishedAt:         now,
		GeneratedTitle:     finalTitle,
		ObservedOutputText: finalTitle,
		ProviderSettlement: providerSettlementForSuccess(*operation.ActiveProviderInvocationID, metadata, now),
		TerminalEvent:      terminalEvent,
	}, nil
}

type invalidRecoveredGeneratedTitleError struct {
	cause error
}

func (e *invalidRecoveredGeneratedTitleError) Error() string {
	if e == nil || e.cause == nil {
		return "invalid recovered generated title"
	}
	return e.cause.Error()
}

func (e *invalidRecoveredGeneratedTitleError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
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
	if r.ctx != nil && r.ctx.Err() != nil {
		return r.cancelOperation(ctx, operation, cursor, "operation cancelled during shutdown")
	}
	if cause == nil {
		cause = fmt.Errorf("planner operation failed")
	}
	errPayload := map[string]any{"error": cause.Error()}
	terminalEvent, eventErr := r.buildTerminalEventFromCursor(ctx, operation.OperationID, cursor, "error", errPayload)
	if eventErr != nil {
		return eventErr
	}
	now := r.deps.nowString()
	providerMetadata := providerMetadataFromError(cause)
	observedOutputText = firstNonEmpty(strings.TrimSpace(observedOutputText), strings.TrimSpace(providerMetadata.FinalText))
	providerSettlement, err := terminalPlanningProviderSettlement(ctx, r.deps.store, operation, providerMetadata, now, cause.Error(), cause, false)
	if err != nil {
		return err
	}
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
	payload := map[string]any{"reason": reason}
	terminalEvent, eventErr := r.buildTerminalEventFromCursor(writeCtx, operation.OperationID, cursor, "cancelled", payload)
	if eventErr != nil {
		return eventErr
	}
	now := r.deps.nowString()
	providerSettlement, err := terminalPlanningProviderSettlement(writeCtx, r.deps.store, operation, ProviderExecutionMetadata{}, now, reason, nil, true)
	if err != nil {
		return err
	}
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

func planningMessagePayload(message state.PlanningMessage) map[string]any {
	payload := map[string]any{
		"planning_message_id": message.PlanningMessageID,
		"planning_session_id": message.PlanningSessionID,
		"turn_index":          message.TurnIndex,
		"role":                message.Role,
		"content_markdown":    message.ContentMarkdown,
		"variant_index":       message.VariantIndex,
		"active":              message.Active,
		"created_at":          message.CreatedAt,
	}
	if question := questionPayloadFromJSON(message.QuestionPayloadJSON); question != nil {
		payload["question_payload"] = structuredQuestionPayload(question)
	}
	if answer := answerPayloadMap(message.AnswerPayloadJSON); len(answer) > 0 {
		payload["answer_payload"] = answer
	}
	return payload
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
