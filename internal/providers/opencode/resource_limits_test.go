package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/processguardian"
	"kingdom/internal/providerlimits"
	"kingdom/internal/testutil/guardianfixture"
)

func TestSessionAdapterStreamsLongPromptThroughStdin(t *testing.T) {
	const promptMarker = "kingdom-stdin-prompt-marker"
	prompt := strings.Repeat("Long decree context 界\n", 240_000) + promptMarker
	if len(prompt) <= 4<<20 {
		t.Fatalf("test prompt bytes=%d, want larger than guardian control frame", len(prompt))
	}
	runner := &promptCaptureRunner{output: successfulProviderEvents("ok")}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}
	const attemptID = "attempt_large_stdin_prompt"
	const invocationID = "invocation_large_stdin_prompt"
	if _, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_large_stdin_prompt",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         prompt,
		InvocationID:   invocationID,
		OccurredAt:     "2026-03-02T10:21:15Z",
	}); err != nil {
		t.Fatalf("execute long prompt: %v", err)
	}
	if got, want := runner.input.String(), strings.Join(providerInvocationPromptParts(prompt, attemptID, invocationID), ""); got != want {
		t.Fatalf("stdin prompt bytes=%d, want exact decorated prompt bytes=%d", len(got), len(want))
	}
	if !runner.input.closed {
		t.Fatal("OpenCode stdin was not closed to deliver EOF")
	}
	for _, arg := range runner.command.Args {
		if strings.Contains(arg, promptMarker) {
			t.Fatal("prompt content appeared in the OpenCode command arguments")
		}
	}
}

func TestSessionAdapterBoundsPromptInputStallsAndPreservesCancellationOutcome(t *testing.T) {
	for _, test := range []struct {
		name             string
		cancel           bool
		partialWrites    int
		partialWriteWait time.Duration
		inactivity       time.Duration
		wantReason       string
		wantEvent        string
	}{
		{name: "stalled input times out", inactivity: 40 * time.Millisecond, wantReason: "timeout", wantEvent: AttemptEventTypeFailed},
		{name: "partial writes count as progress", partialWrites: 4, partialWriteWait: 50 * time.Millisecond, inactivity: 500 * time.Millisecond, wantReason: "timeout", wantEvent: AttemptEventTypeFailed},
		{name: "caller cancellation remains cancellation", cancel: true, inactivity: time.Second, wantReason: "provider_cancelled", wantEvent: AttemptEventTypeCancelled},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := newBlockedPromptRunner(test.partialWrites, test.partialWriteWait)
			adapter, err := NewSessionAdapter(SessionAdapterConfig{
				BinaryPath:              os.Args[0],
				Runner:                  runner,
				FirstEventTimeout:       time.Second,
				StreamInactivityTimeout: test.inactivity,
			})
			if err != nil {
				t.Fatalf("new session adapter: %v", err)
			}
			ctx := context.Background()
			if test.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				go func() {
					time.Sleep(15 * time.Millisecond)
					cancel()
				}()
			}
			response, err := adapter.Execute(ctx, ExecuteRequest{
				CompletionMode: CompletionModeServerResponse,
				SessionID:      "session_prompt_stall_" + strings.ReplaceAll(test.name, " ", "_"),
				AttemptID:      "attempt_prompt_stall_" + strings.ReplaceAll(test.name, " ", "_"),
				RunID:          "run_prompt_stall",
				NodeRunID:      "node_prompt_stall",
				WorkspacePath:  t.TempDir(),
				Prompt:         strings.Repeat("blocked prompt input ", 4096),
				OccurredAt:     "2026-03-02T10:21:15Z",
			})
			if err == nil {
				t.Fatal("expected prompt input interruption")
			}
			if response.Outcome.ReasonCode != test.wantReason {
				t.Fatalf("prompt input outcome=%#v, want reason %q", response.Outcome, test.wantReason)
			}
			if response.StreamObservation.TimedOut != !test.cancel || response.StreamObservation.Cancelled != test.cancel {
				t.Fatalf("prompt input observation timeout=%t cancelled=%t, cancel=%t", response.StreamObservation.TimedOut, response.StreamObservation.Cancelled, test.cancel)
			}
			if !runner.cancelled {
				t.Fatal("stalled/cancelled prompt input did not stop the guarded process")
			}
			if runner.handle.input.partialWritesDone != test.partialWrites {
				t.Fatalf("completed partial writes=%d, want %d before terminal input handling", runner.handle.input.partialWritesDone, test.partialWrites)
			}
			events := eventsFromAttempt(t, adapter, "attempt_prompt_stall_"+strings.ReplaceAll(test.name, " ", "_"))
			if len(events) < 2 || events[len(events)-1].EventType != test.wantEvent {
				t.Fatalf("prompt input terminal events=%#v, want last type %q", events, test.wantEvent)
			}
		})
	}
}

func TestSessionAdapterCanonicalOutputBoundary(t *testing.T) {
	request := func(attemptID string) ExecuteRequest {
		return ExecuteRequest{CompletionMode: CompletionModeServerResponse, SessionID: "session_output_boundary", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", WorkspacePath: t.TempDir(), Prompt: "output boundary", OccurredAt: "2026-03-02T10:21:15Z"}
	}

	runner := guardianfixture.NewOutputRunner(successfulProviderEvents(strings.Repeat("x", providerlimits.CanonicalOutputBytes)), nil)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}
	res, err := adapter.Execute(context.Background(), request("attempt_output_exact"))
	if err != nil {
		t.Fatalf("exact canonical output boundary rejected: %v", err)
	}
	if len(res.OutputText) != providerlimits.CanonicalOutputBytes {
		t.Fatalf("exact canonical output bytes=%d, want %d", len(res.OutputText), providerlimits.CanonicalOutputBytes)
	}

	overflowRunner := guardianfixture.NewOutputRunner(successfulProviderEvents(strings.Repeat("x", providerlimits.CanonicalOutputBytes+1)), nil)
	overflowAdapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: overflowRunner})
	if err != nil {
		t.Fatalf("new overflow session adapter: %v", err)
	}
	_, err = overflowAdapter.Execute(context.Background(), request("attempt_output_overflow"))
	var limitErr *providerlimits.LimitExceededError
	if !errors.As(err, &limitErr) || limitErr.Resource != "canonical output" || !limitErr.NonRetryable() {
		t.Fatalf("one-byte canonical output overflow error=%v, want non-retryable canonical output limit error", err)
	}
}

func TestSessionAdapterRejectsLargeMalformedEventWithoutLeakingContent(t *testing.T) {
	runner := guardianfixture.NewOutputRunner(append([]byte(`{"type":"step_start"}`+"\n"), []byte(strings.Repeat("x", providerlimits.OpenCodeEventLineBytes+1))...), nil)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}
	res, err := adapter.Execute(context.Background(), ExecuteRequest{CompletionMode: CompletionModeServerResponse, SessionID: "session_malformed_overflow", AttemptID: "attempt_malformed_overflow", RunID: "run_1", NodeRunID: "nr_1", WorkspacePath: t.TempDir(), Prompt: "large malformed event", OccurredAt: "2026-03-02T10:21:15Z"})
	if err == nil {
		t.Fatal("expected oversized malformed event to fail")
	}
	if res.Outcome.ReasonCode != "resource_limit_exceeded" {
		t.Fatalf("oversized malformed event outcome=%#v", res.Outcome)
	}
	if strings.Contains(err.Error(), strings.Repeat("x", 32)) {
		t.Fatalf("oversized malformed event leaked provider content: %v", err)
	}
}

func TestSessionAdapterCapturesTailOfUnterminatedOversizedStderr(t *testing.T) {
	const marker = "unterminated-stderr-tail"
	runner := guardianfixture.NewOutputRunner(successfulProviderEvents("ok"), []byte(strings.Repeat("x", providerlimits.StderrTailBytes+1)+marker))
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}
	res, err := adapter.Execute(context.Background(), ExecuteRequest{CompletionMode: CompletionModeServerResponse, SessionID: "session_stderr_tail", AttemptID: "attempt_stderr_tail", RunID: "run_1", NodeRunID: "nr_1", WorkspacePath: t.TempDir(), Prompt: "unterminated stderr", OccurredAt: "2026-03-02T10:21:15Z"})
	if err != nil {
		t.Fatalf("execute with diagnostic stderr: %v", err)
	}
	assertProviderDiagnosticsStderr(t, res.ProviderDiagnosticsJSON, marker)
	var diagnostics struct {
		StderrTail string `json:"stderr_tail"`
	}
	if err := json.Unmarshal([]byte(res.ProviderDiagnosticsJSON), &diagnostics); err != nil {
		t.Fatalf("decode provider diagnostics: %v", err)
	}
	if len(diagnostics.StderrTail) > providerlimits.StderrTailBytes {
		t.Fatalf("stderr tail bytes=%d, want <= %d", len(diagnostics.StderrTail), providerlimits.StderrTailBytes)
	}
}

func TestRunGuardedCommandReadBoundary(t *testing.T) {
	runner := guardianfixture.NewOutputRunner([]byte(strings.Repeat("x", int(providerlimits.GuardedCommandStreamBytes))), []byte(strings.Repeat("y", int(providerlimits.GuardedCommandStreamBytes))))
	stdout, stderr, _, err := runGuardedCommand(context.Background(), runner, processguardian.Command{Path: "fixture"})
	if err != nil {
		t.Fatalf("exact guarded command boundary rejected: %v", err)
	}
	if len(stdout) != int(providerlimits.GuardedCommandStreamBytes) || len(stderr) != int(providerlimits.GuardedCommandStreamBytes) {
		t.Fatalf("exact guarded command output sizes=(%d,%d)", len(stdout), len(stderr))
	}
	overflowRunner := guardianfixture.NewOutputRunner([]byte(strings.Repeat("x", int(providerlimits.GuardedCommandStreamBytes+1))), nil)
	_, _, _, err = runGuardedCommand(context.Background(), overflowRunner, processguardian.Command{Path: "fixture"})
	var limitErr *providerlimits.LimitExceededError
	if !errors.As(err, &limitErr) || !limitErr.NonRetryable() {
		t.Fatalf("one-byte guarded command overflow error=%v, want non-retryable limit error", err)
	}
	if overflowRunner.Cancels() != 1 {
		t.Fatalf("guarded command overflow cancellations=%d, want 1", overflowRunner.Cancels())
	}
}

func successfulProviderEvents(text string) []byte {
	return []byte(`{"type":"step_start"}` + "\n" + `{"type":"text","part":{"text":"` + text + `"}}` + "\n" + `{"type":"step_finish","part":{"reason":"stop"}}` + "\n")
}

type promptCaptureRunner struct {
	command processguardian.Command
	input   promptCaptureWriteCloser
	output  []byte
}

func (r *promptCaptureRunner) Start(_ context.Context, command processguardian.Command, _ processguardian.PersistOwnership) (processguardian.Handle, error) {
	r.command = command
	return &promptCaptureHandle{runner: r}, nil
}

type promptCaptureHandle struct {
	runner *promptCaptureRunner
}

func (*promptCaptureHandle) GuardID() string         { return "guard_prompt_capture" }
func (h *promptCaptureHandle) Stdin() io.WriteCloser { return &h.runner.input }
func (h *promptCaptureHandle) Stdout() io.ReadCloser {
	return io.NopCloser(strings.NewReader(string(h.runner.output)))
}
func (*promptCaptureHandle) Stderr() io.ReadCloser        { return io.NopCloser(strings.NewReader("")) }
func (*promptCaptureHandle) Cancel(context.Context) error { return nil }
func (h *promptCaptureHandle) Wait(context.Context) (processguardian.Result, error) {
	if !h.runner.input.closed {
		return processguardian.Result{}, errors.New("prompt stdin was not closed")
	}
	return processguardian.Result{Quiescent: true}, nil
}

type promptCaptureWriteCloser struct {
	strings.Builder
	closed bool
}

func (w *promptCaptureWriteCloser) Write(payload []byte) (int, error) {
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	return w.Builder.Write(payload)
}

func (w *promptCaptureWriteCloser) Close() error {
	w.closed = true
	return nil
}

type blockedPromptRunner struct {
	mu        sync.Mutex
	cancelled bool
	handle    *blockedPromptHandle
}

func newBlockedPromptRunner(partialWrites int, partialWriteWait time.Duration) *blockedPromptRunner {
	runner := &blockedPromptRunner{}
	runner.handle = &blockedPromptHandle{runner: runner, input: &blockedPromptInput{closed: make(chan struct{}), partialWrites: partialWrites, partialWriteWait: partialWriteWait}}
	return runner
}

func (r *blockedPromptRunner) Start(context.Context, processguardian.Command, processguardian.PersistOwnership) (processguardian.Handle, error) {
	return r.handle, nil
}

type blockedPromptHandle struct {
	runner *blockedPromptRunner
	input  *blockedPromptInput
}

func (*blockedPromptHandle) GuardID() string         { return "guard_blocked_prompt" }
func (h *blockedPromptHandle) Stdin() io.WriteCloser { return h.input }
func (*blockedPromptHandle) Stdout() io.ReadCloser   { return io.NopCloser(strings.NewReader("")) }
func (*blockedPromptHandle) Stderr() io.ReadCloser   { return io.NopCloser(strings.NewReader("")) }
func (h *blockedPromptHandle) Cancel(context.Context) error {
	h.runner.mu.Lock()
	h.runner.cancelled = true
	h.runner.mu.Unlock()
	return h.input.Close()
}
func (*blockedPromptHandle) Wait(context.Context) (processguardian.Result, error) {
	return processguardian.Result{Quiescent: true}, nil
}

type blockedPromptInput struct {
	closed            chan struct{}
	once              sync.Once
	mu                sync.Mutex
	partialWrites     int
	partialWriteWait  time.Duration
	partialWritesDone int
	initialFullWrite  bool
}

func (w *blockedPromptInput) Write(payload []byte) (int, error) {
	w.mu.Lock()
	partial := w.partialWrites > 0
	if partial {
		w.partialWrites--
	}
	if !partial && w.partialWriteWait == 0 && !w.initialFullWrite {
		w.initialFullWrite = true
		w.mu.Unlock()
		return len(payload), nil
	}
	delay := w.partialWriteWait
	w.mu.Unlock()
	if partial {
		time.Sleep(delay)
		select {
		case <-w.closed:
			return 0, io.ErrClosedPipe
		default:
		}
		written := min(len(payload), 1024)
		w.mu.Lock()
		w.partialWritesDone++
		w.mu.Unlock()
		return written, nil
	}
	<-w.closed
	return 0, io.ErrClosedPipe
}

func (w *blockedPromptInput) Close() error {
	w.once.Do(func() { close(w.closed) })
	return nil
}

func TestRunGuardedCommandCancelsOverflowAndClosesStreamBeforeQuiescence(t *testing.T) {
	cancelled := make(chan struct{})
	reader := &continuedOutputReader{remaining: providerlimits.GuardedCommandStreamBytes + 1, cancelled: cancelled}
	stdout := &continuedOutputStream{Reader: reader, closed: make(chan struct{})}
	handle := &continuedOutputHandle{stdout: stdout, stderr: io.NopCloser(strings.NewReader("")), cancelled: cancelled, stdoutClosed: stdout.closed}
	runner := continuedOutputRunner{handle: handle}

	_, _, result, err := runGuardedCommand(context.Background(), runner, processguardian.Command{Path: "continued-output"})
	var limitErr *providerlimits.LimitExceededError
	if !errors.As(err, &limitErr) {
		t.Fatalf("continued output error=%v, want limit error", err)
	}
	if !handle.cancelObserved {
		t.Fatal("overflow did not request cancellation")
	}
	select {
	case <-stdout.closed:
	default:
		t.Fatal("guarded command returned without closing its stdout stream")
	}
	if reader.continued {
		t.Fatal("guarded command continued reading stdout after the first overflow byte")
	}
	if !result.Quiescent {
		t.Fatalf("overflow returned without guardian quiescence: %#v", result)
	}
}

func TestRunBoundedGuardedCommandBoundsCancellationQuiescenceAndPipeDrains(t *testing.T) {
	const (
		budget        = 250 * time.Millisecond
		cancelReserve = 100 * time.Millisecond
		tolerance     = 100 * time.Millisecond
	)
	stdout := newBlockedGuardedCommandStream()
	stderr := newBlockedGuardedCommandStream()
	handle := &blockedBoundedCommandHandle{stdout: stdout, stderr: stderr, cancelled: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	startedAt := time.Now()
	_, _, result, err := runBoundedGuardedCommand(ctx, blockedBoundedCommandRunner{handle: handle}, processguardian.Command{Path: "blocked-diagnostic"}, cancelReserve)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked bounded command error=%v, want deadline exceeded", err)
	}
	if result.Quiescent {
		t.Fatalf("blocked bounded command reported unproven quiescence as success: %#v", result)
	}
	if elapsed := time.Since(startedAt); elapsed > budget+tolerance {
		t.Fatalf("blocked bounded command exceeded %s budget plus %s scheduler tolerance: %s", budget, tolerance, elapsed)
	}
	select {
	case <-handle.cancelled:
	default:
		t.Fatal("blocked bounded command did not request cancellation")
	}
	for _, stream := range []*blockedGuardedCommandStream{stdout, stderr} {
		select {
		case <-stream.drained:
		case <-time.After(tolerance):
			t.Fatal("blocked bounded command left a pipe reader blocked after closing its stream")
		}
	}
}

type continuedOutputRunner struct {
	handle *continuedOutputHandle
}

type blockedBoundedCommandRunner struct {
	handle *blockedBoundedCommandHandle
}

func (r blockedBoundedCommandRunner) Start(context.Context, processguardian.Command, processguardian.PersistOwnership) (processguardian.Handle, error) {
	return r.handle, nil
}

type blockedBoundedCommandHandle struct {
	stdout     *blockedGuardedCommandStream
	stderr     *blockedGuardedCommandStream
	cancelled  chan struct{}
	cancelOnce sync.Once
}

func (h *blockedBoundedCommandHandle) GuardID() string       { return "blocked-diagnostic" }
func (*blockedBoundedCommandHandle) Stdin() io.WriteCloser   { return unusedWriteCloser{} }
func (h *blockedBoundedCommandHandle) Stdout() io.ReadCloser { return h.stdout }
func (h *blockedBoundedCommandHandle) Stderr() io.ReadCloser { return h.stderr }
func (h *blockedBoundedCommandHandle) Cancel(context.Context) error {
	h.cancelOnce.Do(func() { close(h.cancelled) })
	return nil
}
func (h *blockedBoundedCommandHandle) Wait(ctx context.Context) (processguardian.Result, error) {
	select {
	case <-h.cancelled:
	case <-ctx.Done():
		return processguardian.Result{}, ctx.Err()
	}
	<-ctx.Done()
	return processguardian.Result{}, ctx.Err()
}

type blockedGuardedCommandStream struct {
	closed    chan struct{}
	drained   chan struct{}
	closeOnce sync.Once
	drainOnce sync.Once
}

func newBlockedGuardedCommandStream() *blockedGuardedCommandStream {
	return &blockedGuardedCommandStream{closed: make(chan struct{}), drained: make(chan struct{})}
}

func (s *blockedGuardedCommandStream) Read([]byte) (int, error) {
	<-s.closed
	s.drainOnce.Do(func() { close(s.drained) })
	return 0, io.EOF
}

func (s *blockedGuardedCommandStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func (r continuedOutputRunner) Start(context.Context, processguardian.Command, processguardian.PersistOwnership) (processguardian.Handle, error) {
	return r.handle, nil
}

type continuedOutputStream struct {
	io.Reader
	closed chan struct{}
	once   sync.Once
}

func (s *continuedOutputStream) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

type continuedOutputHandle struct {
	stdout         io.ReadCloser
	stderr         io.ReadCloser
	cancelled      chan struct{}
	stdoutClosed   <-chan struct{}
	cancelOnce     sync.Once
	cancelObserved bool
}

func (h *continuedOutputHandle) GuardID() string       { return "continued-output" }
func (*continuedOutputHandle) Stdin() io.WriteCloser   { return unusedWriteCloser{} }
func (h *continuedOutputHandle) Stdout() io.ReadCloser { return h.stdout }
func (h *continuedOutputHandle) Stderr() io.ReadCloser { return h.stderr }
func (h *continuedOutputHandle) Cancel(context.Context) error {
	h.cancelOnce.Do(func() {
		h.cancelObserved = true
		close(h.cancelled)
	})
	return nil
}
func (h *continuedOutputHandle) Wait(context.Context) (processguardian.Result, error) {
	<-h.cancelled
	<-h.stdoutClosed
	return processguardian.Result{Quiescent: true}, nil
}

type unusedWriteCloser struct{}

func (unusedWriteCloser) Write(payload []byte) (int, error) { return len(payload), nil }
func (unusedWriteCloser) Close() error                      { return nil }

type continuedOutputReader struct {
	remaining int64
	cancelled <-chan struct{}
	continued bool
}

func (r *continuedOutputReader) Read(buffer []byte) (int, error) {
	if r.remaining > 0 {
		count := min(int64(len(buffer)), r.remaining)
		for index := range buffer[:count] {
			buffer[index] = 'x'
		}
		r.remaining -= count
		return int(count), nil
	}
	if !r.continued {
		<-r.cancelled
		r.continued = true
		buffer[0] = 'y'
		return 1, nil
	}
	return 0, io.EOF
}

func TestProviderStreamBudgetAggregateBoundaries(t *testing.T) {
	budget := providerStreamBudget{}
	if err := budget.consumeEvent(providerlimits.OpenCodeEventBytes); err != nil {
		t.Fatalf("exact aggregate event-byte boundary rejected: %v", err)
	}
	var byteLimitErr *providerlimits.LimitExceededError
	if err := budget.consumeEvent(1); !errors.As(err, &byteLimitErr) {
		t.Fatalf("one-byte aggregate event overflow error=%v, want limit error", err)
	}

	budget = providerStreamBudget{}
	for range providerlimits.OpenCodeEventCount {
		if err := budget.consumeEvent(1); err != nil {
			t.Fatalf("exact aggregate event-count boundary rejected: %v", err)
		}
	}
	var countLimitErr *providerlimits.CountExceededError
	if err := budget.consumeEvent(1); !errors.As(err, &countLimitErr) {
		t.Fatalf("one-event aggregate overflow error=%v, want count limit error", err)
	}

	budget = providerStreamBudget{}
	if err := budget.consumeOutput(strings.Repeat("x", providerlimits.CanonicalOutputBytes-2)); err != nil {
		t.Fatalf("aggregate output prefix rejected: %v", err)
	}
	if err := budget.consumeOutput("x"); err != nil {
		t.Fatalf("exact aggregate output boundary rejected: %v", err)
	}
	var outputLimitErr *providerlimits.LimitExceededError
	if err := budget.consumeOutput("x"); !errors.As(err, &outputLimitErr) || outputLimitErr.Resource != "canonical output" {
		t.Fatalf("one-byte aggregate output overflow error=%v, want canonical output limit error", err)
	}
}

func TestResourceLimitFailuresUseCanonicalOutcome(t *testing.T) {
	t.Parallel()

	for _, err := range []error{
		&providerlimits.LimitExceededError{Resource: "canonical output", Limit: providerlimits.CanonicalOutputBytes},
		&providerlimits.LimitExceededError{Resource: "event line", Limit: providerlimits.OpenCodeEventLineBytes},
		&providerlimits.LimitExceededError{Resource: "event stream", Limit: providerlimits.OpenCodeEventBytes},
		&providerlimits.CountExceededError{Resource: "event stream", Limit: providerlimits.OpenCodeEventCount},
	} {
		result := executionResultForResourceLimit(err, "session_resource_limit")
		if result.State != attemptCompletionStateFailed || result.ReasonCode != "resource_limit_exceeded" || result.FailureText != err.Error() {
			t.Fatalf("resource limit result=%+v for %v", result, err)
		}
	}
}
