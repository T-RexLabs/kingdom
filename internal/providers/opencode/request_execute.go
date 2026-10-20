package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/providers/opencode/protocol"
)

// promptSendLimit bounds how long Execute keeps retrying to hand the prompt to
// the server; past it the server is not serving this request.
const promptSendLimit = 30 * time.Second

// stopCause records why Execute interrupted a request, if it did.
type stopCause int

const (
	stopNone stopCause = iota
	stopCancelled
	stopTimedOut
)

// Execute sends one prepared request and returns only evidence that the
// protocol verifier proved. Provider failures, cancellations and timeouts are
// reported through that evidence; an error means the request could not be
// proven and its canonical invocation must stay open.
func (s *Service) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	prepared := s.requests.registry.invocation(req.InvocationID)
	if prepared == nil {
		return ExecuteResponse{}, fmt.Errorf("provider invocation %s was not prepared", req.InvocationID)
	}
	if (req.OnCommandGuardianOwnership == nil) != (req.OnCommandGuardianQuiescence == nil) {
		return ExecuteResponse{}, fmt.Errorf("command guardian callbacks must be set together")
	}
	prepared.mu.Lock()
	if prepared.executing {
		prepared.mu.Unlock()
		return ExecuteResponse{}, fmt.Errorf("provider invocation %s is already executing", req.InvocationID)
	}
	prepared.executing = true
	prepared.mu.Unlock()
	defer func() {
		s.requests.registry.remove(prepared)
		close(prepared.done)
	}()

	run := &execution{service: s, request: prepared, req: req}
	return run.execute(ctx)
}

type execution struct {
	service *Service
	request *request
	req     ExecuteRequest
	cause   stopCause
}

func (e *execution) client() *Client { return e.request.started.client }

func (e *execution) capture() *capture { return e.request.started.capture }

func (e *execution) identity() providerexecution.RequestIdentity { return e.request.identity }

// bounded returns a context for a phase that must finish even after the caller
// gives up, and that is bounded by its own limit rather than by the request's
// total duration.
func (e *execution) bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), e.service.requests.settleLimit)
}

func (e *execution) execute(ctx context.Context) (response ExecuteResponse, err error) {
	runtime := e.service.requests
	authority := runtime.authority
	defer func() {
		retire, cancel := e.bounded(ctx)
		defer cancel()
		if err := retireProfile(retire, e.client(), e.request.directory, e.request.profile.id); err != nil {
			slog.Warn("OpenCode profile retirement deferred", "profile_id", e.request.profile.id, "error", err)
		}
		// Only a session that ended in a proven success can be continued.
		if !e.req.KeepSession || err != nil || response.Outcome.State != "success" {
			e.capture().release(e.identity().SessionID)
		}
	}()

	prompt, err := e.prompt(ctx)
	if err != nil {
		return e.abandon(ctx, err)
	}
	if e.req.OnCommandGuardianOwnership != nil {
		registration := &commandRegistration{workspace: e.request.directory, ownership: e.req.OnCommandGuardianOwnership, quiescence: e.req.OnCommandGuardianQuiescence}
		if err := runtime.commands.register(e.identity(), registration); err != nil {
			return ExecuteResponse{}, err
		}
		defer runtime.commands.unregister(e.identity())
	}

	if e.stopRequested() {
		return e.abandon(ctx, context.Canceled)
	}
	if err := authority.IssueDispatch(ctx, e.identity()); err != nil {
		return e.abandon(ctx, err)
	}
	feed, err := e.capture().follow(e.identity().SessionID)
	if err != nil {
		return ExecuteResponse{}, errors.Join(err, e.stopBounded(ctx))
	}
	progress := e.consume(feed)
	defer progress.wait()
	defer feed.close()

	if err := e.sendPrompt(ctx, prompt); err != nil {
		// Whether the server received the prompt is unknown, but fencing the
		// request first means nothing it does can change the workspace, so the
		// request can be abandoned once its admitted effects have closed.
		cause := fmt.Errorf("send OpenCode prompt: %w", err)
		if stopErr := e.stopBounded(ctx); stopErr != nil {
			return ExecuteResponse{}, errors.Join(cause, stopErr)
		}
		if e.stopRequested() {
			cause = errors.Join(cause, context.Canceled)
		}
		return e.abandon(ctx, cause)
	}
	invokedAt := time.Now().UTC().Format(time.RFC3339Nano)
	if e.req.OnInvocationStarted != nil {
		e.req.OnInvocationStarted()
	}
	if err := e.await(ctx); err != nil {
		return ExecuteResponse{}, err
	}
	settle, cancel := e.bounded(ctx)
	defer cancel()
	return e.finalize(settle, invokedAt)
}

// abandon closes a request that ended without an answer Kingdom can prove, and
// returns the abandoned terminal so the attempt can settle. The caller has
// either sent nothing or has already fenced admission, and abandonment itself
// succeeds only once every admitted effect is closed.
func (e *execution) abandon(ctx context.Context, cause error) (ExecuteResponse, error) {
	fence, cancel := e.bounded(ctx)
	defer cancel()
	if err := e.service.requests.authority.Abandon(fence, e.identity()); err != nil {
		return ExecuteResponse{}, errors.Join(cause, err)
	}
	observation, err := providerexecution.AbandonedObservation(e.identity(), e.req.InvocationID)
	if err != nil {
		return ExecuteResponse{}, errors.Join(cause, err)
	}
	outcome := ExecutionOutcome{State: "failed", ReasonCode: string(providerexecution.CanonicalProviderErrorCode("", "", 0)), FailureText: cause.Error(), EvidenceSource: "request"}
	if errors.Is(cause, context.Canceled) || errors.Is(cause, providerrequest.ErrDispatchClosed) {
		observation.Cancelled = true
		outcome = ExecutionOutcome{State: "cancelled", ReasonCode: "provider_cancelled", EvidenceSource: "request"}
	}
	return ExecuteResponse{
		ProviderSessionID: e.identity().SessionID, ProviderInvocationID: e.req.InvocationID,
		StreamObservation: observation, Outcome: outcome,
	}, cause
}

// sendPrompt retries transport failures for a bounded time. The server dedupes a
// prompt by its message ID, so repeating an admitted prompt cannot send the work
// twice. A stop request ends the retries at once.
func (e *execution) sendPrompt(ctx context.Context, text string) error {
	sendCtx, cancel := context.WithTimeout(ctx, promptSendLimit)
	defer cancel()
	go func() {
		select {
		case <-e.request.stop:
			cancel()
		case <-sendCtx.Done():
		}
	}()
	return retryTransient(sendCtx, func() error {
		return e.client().Prompt(sendCtx, e.request.directory, e.identity().SessionID, e.identity().RequestID, text)
	})
}

func (e *execution) stopRequested() bool {
	select {
	case <-e.request.stop:
		return true
	default:
		return false
	}
}

// prompt reads the input this request submits: only the new input when it
// continues a session that already holds the earlier history.
func (e *execution) prompt(ctx context.Context) (string, error) {
	open := e.req.OpenPrompt
	if e.request.reused {
		open = e.req.OpenContinuationPrompt
	}
	if open == nil {
		return "", fmt.Errorf("provider request has no prompt source")
	}
	reader, err := open(ctx)
	if err != nil {
		return "", err
	}
	raw, readErr := io.ReadAll(reader)
	if err := errors.Join(readErr, reader.Close()); err != nil {
		return "", err
	}
	return string(raw), nil
}

// await waits for the session to become idle, interrupting it on cancellation
// or silence. Waiting never ends the request by itself: only the verified
// evidence does, and it has no total duration limit of its own.
func (e *execution) await(ctx context.Context) error {
	waitCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	waited := make(chan error, 1)
	go func() { waited <- e.client().Wait(waitCtx, e.request.directory, e.identity().SessionID) }()
	watchdog := time.NewTicker(watchdogInterval(e.req.FirstEventTimeout, e.req.StreamInactivityTimeout))
	defer watchdog.Stop()
	for {
		select {
		case err := <-waited:
			if err != nil {
				return errors.Join(fmt.Errorf("wait for OpenCode request: %w", err), e.stopBounded(ctx))
			}
			return nil
		case <-ctx.Done():
			e.cause = stopCancelled
		case <-e.request.stop:
			e.cause = stopCancelled
		case <-watchdog.C:
			if e.silent() {
				e.cause = stopTimedOut
			}
		}
		if e.cause != stopNone {
			if err := e.stopBounded(ctx); err != nil {
				return err
			}
			drain, cancelDrain := e.bounded(ctx)
			defer cancelDrain()
			select {
			case err := <-waited:
				if err != nil {
					return fmt.Errorf("wait for interrupted OpenCode request: %w", err)
				}
				return nil
			case <-drain.Done():
				return drain.Err()
			}
		}
	}
}

func watchdogInterval(limits ...time.Duration) time.Duration {
	interval := time.Second
	for _, limit := range limits {
		if limit > 0 && limit/4 < interval {
			interval = limit / 4
		}
	}
	return max(interval, 10*time.Millisecond)
}

// silent applies the first-event limit until the server has said anything, and
// the inactivity limit afterwards.
func (e *execution) silent() bool {
	quiet, spoke := e.capture().quiet(e.identity().SessionID)
	limit := e.req.FirstEventTimeout
	if spoke {
		limit = e.req.StreamInactivityTimeout
	}
	return limit > 0 && quiet > limit
}

func (e *execution) stopBounded(ctx context.Context) error {
	stopCtx, cancel := e.bounded(ctx)
	defer cancel()
	return e.stop(stopCtx)
}

// stop fences the request and interrupts it. Admission closes first, so nothing
// new can start. Every step is attempted even if an earlier one failed, because
// a failed fence must not leave the request running.
func (e *execution) stop(ctx context.Context) error {
	runtime := e.service.requests
	fenceErr := runtime.authority.CloseAdmission(ctx, e.identity())
	interruptErr := retryTransient(ctx, func() error {
		return e.client().Interrupt(ctx, e.request.directory, e.identity().SessionID)
	})
	return errors.Join(fenceErr, interruptErr, runtime.commands.cancel(ctx, e.identity()))
}

// finalize proves the terminal state from captured events and the projected
// session, records it, and returns it as the response.
func (e *execution) finalize(ctx context.Context, invokedAt string) (ExecuteResponse, error) {
	runtime := e.service.requests
	identity := e.identity()
	invocationID := e.req.InvocationID
	if err := runtime.authority.CloseAdmission(ctx, identity); err != nil {
		return ExecuteResponse{}, fmt.Errorf("close OpenCode admission: %w", err)
	}
	commandErr := runtime.commands.cancel(ctx, identity)
	if err := runtime.recoverFor(ctx, identity); err != nil {
		return ExecuteResponse{}, fmt.Errorf("recover native receipts: %w", err)
	}
	watermark, err := e.client().Log(ctx, e.request.directory, identity.SessionID, func(json.RawMessage) error { return nil })
	if err != nil {
		return ExecuteResponse{}, err
	}
	if err := e.capture().await(ctx, identity.SessionID, watermark); err != nil {
		return ExecuteResponse{}, err
	}
	var staged []*stagedWriter
	discard := func() {
		for _, writer := range staged {
			writer.discard()
		}
	}
	writer := func(pattern string) (*stagedWriter, error) {
		w, err := newStagedWriter(pattern)
		if err == nil {
			staged = append(staged, w)
		}
		return w, err
	}
	events, err := writer("kingdom-provider-events-*")
	if err != nil {
		return ExecuteResponse{}, err
	}
	if err := e.capture().writeInterval(identity.SessionID, identity.RequestID, watermark, events); err != nil {
		discard()
		return ExecuteResponse{}, err
	}
	messages, err := writer("kingdom-provider-messages-*")
	if err != nil {
		discard()
		return ExecuteResponse{}, err
	}
	if err := e.writeMessages(ctx, messages); err != nil {
		discard()
		return ExecuteResponse{}, err
	}
	text, err := writer("kingdom-provider-text-*")
	if err != nil {
		discard()
		return ExecuteResponse{}, err
	}
	eventsContent, err := events.finish(invocationID)
	if err != nil {
		discard()
		return ExecuteResponse{}, err
	}
	messagesContent, err := messages.finish(invocationID)
	if err != nil {
		discard()
		return ExecuteResponse{}, err
	}
	verified, err := protocol.Verify(ctx, identity, protocol.Evidence{
		InvocationID: invocationID, Events: stagedSource(eventsContent), Messages: stagedSource(messagesContent),
		Watermark: watermark, CaptureHealthy: e.capture().healthy() == nil, WaitCompleted: true,
	}, text)
	if err != nil {
		discard()
		return ExecuteResponse{}, fmt.Errorf("verify OpenCode request: %w", err)
	}
	textContent, err := text.finish(invocationID)
	if err != nil {
		discard()
		return ExecuteResponse{}, err
	}
	// The terminal becomes the request's evidence when it settles. Checking it
	// here keeps an inconsistent record from ever leaving the provider.
	if err := runtime.authority.CheckTerminal(ctx, identity, verified.Terminal); err != nil {
		discard()
		return ExecuteResponse{}, fmt.Errorf("check OpenCode terminal evidence: %w", err)
	}
	return e.response(ctx, verified, eventsContent, messagesContent, textContent, invokedAt, commandErr)
}

// writeMessages writes the projected messages from this request's user input
// through the final idle as a JSON array, oldest first.
func (e *execution) writeMessages(ctx context.Context, out io.Writer) error {
	var newestFirst []json.RawMessage
	found := false
	err := e.client().Messages(ctx, e.request.directory, e.identity().SessionID, func(raw json.RawMessage) error {
		newestFirst = append(newestFirst, raw)
		var message struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &message); err != nil {
			return err
		}
		if message.ID == e.identity().RequestID {
			found = true
			return errStopVisit
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("OpenCode request message was not found in the session")
	}
	if _, err := io.WriteString(out, "["); err != nil {
		return err
	}
	for i := len(newestFirst) - 1; i >= 0; i-- {
		if _, err := out.Write(newestFirst[i]); err != nil {
			return err
		}
		if i > 0 {
			if _, err := io.WriteString(out, ","); err != nil {
				return err
			}
		}
	}
	_, err = io.WriteString(out, "]")
	return err
}

func (e *execution) response(ctx context.Context, verified protocol.Verified, events, messages, text providerexecution.StagedContent, invokedAt string, commandErr error) (ExecuteResponse, error) {
	terminal := verified.Terminal
	staged := []providerexecution.StagedContent{events, messages}
	observation := providerexecution.InvocationStreamObservation{
		Request:           &providerexecution.RequestObservation{Identity: e.identity(), Terminal: terminal},
		ProviderSessionID: e.identity().SessionID, FinalTurnID: terminal.AssistantMessageID,
		RawFinishReason: terminal.RawFinishReason, NormalizedFinishReason: terminal.FinishReason,
		DiagnosticContent:       []providerexecution.ContentReference{events.Reference, messages.Reference},
		StagedDiagnosticContent: staged,
		ToolCalls:               terminal.ToolCalls(), Tokens: verified.Tokens,
		TimedOut:           e.cause == stopTimedOut,
		Cancelled:          terminal.Outcome == providerexecution.RequestOutcomeInterrupted && e.cause != stopTimedOut,
		MeaningfulProgress: terminal.Text != nil || len(terminal.Tools) > 0,
	}
	for _, tool := range terminal.Tools {
		if tool.Status != "completed" && tool.Status != "error" {
			observation.PendingToolCount++
		}
	}
	if failure := verified.Provider; failure != nil {
		observation.ProviderErrorPresent, observation.ProviderError = true, failure.Message
		observation.ProviderErrorCode, observation.ProviderErrorStatus = failure.Code, failure.Status
	}
	response := ExecuteResponse{
		Tokens: verified.Tokens, ProviderSessionID: e.identity().SessionID,
		ProviderInvocationID: e.req.InvocationID, ProviderInvocationAt: invokedAt,
	}
	if terminal.Text != nil {
		observation.CanonicalTerminalTextRef = terminal.Text
		observation.StagedTerminalContent = &text
		observation.TerminalTextOpener = text.Open
		response.OutputPreview = previewOf(ctx, text)
	} else {
		_ = os.Remove(text.Path)
	}
	observation.ReleaseContent = func() error {
		paths := []string{events.Path, messages.Path}
		if terminal.Text != nil {
			paths = append(paths, text.Path)
		}
		var failures []error
		for _, path := range paths {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				failures = append(failures, err)
			}
		}
		return errors.Join(failures...)
	}
	response.StreamObservation = observation
	response.Outcome = e.outcome(terminal, verified.Provider)
	switch e.cause {
	case stopCancelled:
		return response, errors.Join(commandErr, context.Canceled)
	case stopTimedOut:
		return response, errors.Join(commandErr, fmt.Errorf("provider request was silent beyond its limit: %w", context.DeadlineExceeded))
	}
	return response, commandErr
}

// outcome reports the verified terminal as the execution outcome that callers
// persist, so no one re-derives reason codes from error text.
func (e *execution) outcome(terminal providerexecution.RequestTerminal, failure *protocol.ProviderFailure) ExecutionOutcome {
	switch {
	case e.cause == stopTimedOut:
		return ExecutionOutcome{State: "failed", ReasonCode: "timeout", FailureText: "The provider request was silent beyond its limit.", EvidenceSource: "request"}
	case terminal.Outcome == providerexecution.RequestOutcomeInterrupted:
		return ExecutionOutcome{State: "cancelled", ReasonCode: "provider_cancelled", EvidenceSource: "request"}
	case terminal.Outcome == providerexecution.RequestOutcomeFailed:
		outcome := ExecutionOutcome{State: "failed", ReasonCode: string(providerexecution.CanonicalProviderErrorCode("", "", 0)), EvidenceSource: "request"}
		if failure != nil {
			outcome.ReasonCode = string(providerexecution.CanonicalProviderErrorCode(failure.Code, "", failure.Status))
			outcome.FailureText = failure.Message
		}
		return outcome
	}
	return ExecutionOutcome{State: "success", ReasonCode: "execution_completed", EvidenceSource: "request"}
}

// previewOf reads the bounded display prefix of staged text.
func previewOf(ctx context.Context, content providerexecution.StagedContent) string {
	reader, err := content.Open(ctx)
	if err != nil {
		return ""
	}
	summary, _ := providerexecution.InspectText(ctx, reader)
	_ = reader.Close()
	return summary.Preview
}

// consume forwards live text and activity until the feed closes.
func (e *execution) consume(feed *eventFeed) *feedConsumer {
	consumer := &feedConsumer{done: make(chan struct{})}
	var activity *activityProjector
	if e.req.OnActivity != nil {
		activity = newActivityProjector(e.req.InvocationID, e.req.OnActivity)
	}
	go func() {
		defer close(consumer.done)
		for {
			raw, ok := feed.next(context.Background())
			if !ok {
				return
			}
			if activity != nil {
				activity.project(raw)
			}
			if e.req.OnProgress != nil {
				if delta := textDelta(raw); delta != "" {
					e.req.OnProgress(delta)
				}
			}
		}
	}()
	return consumer
}

func textDelta(raw json.RawMessage) string {
	var event struct {
		Type string `json:"type"`
		Data struct {
			Delta string `json:"delta"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &event) != nil || event.Type != "session.text.delta" {
		return ""
	}
	return event.Data.Delta
}

type feedConsumer struct{ done chan struct{} }

func (c *feedConsumer) wait() { <-c.done }
