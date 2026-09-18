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

func TestSessionAdapterRejectsOversizedPromptBeforeLaunch(t *testing.T) {
	runner := guardianfixture.NewOutputRunner(nil, nil)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}
	_, err = adapter.Execute(context.Background(), ExecuteRequest{CompletionMode: CompletionModeServerResponse, SessionID: "session_prompt_overflow", AttemptID: "attempt_prompt_overflow", RunID: "run_1", NodeRunID: "nr_1", WorkspacePath: t.TempDir(), Prompt: strings.Repeat("x", providerlimits.PromptBytes+1), OccurredAt: "2026-03-02T10:21:15Z"})
	if err == nil {
		t.Fatal("expected oversized prompt to fail")
	}
	var limitErr *providerlimits.LimitExceededError
	if !errors.As(err, &limitErr) || limitErr.Resource != "prompt" {
		t.Fatalf("oversized prompt error=%v, want typed prompt limit error", err)
	}
	if runner.Starts() != 0 {
		t.Fatalf("oversized prompt reached provider launch %d times", runner.Starts())
	}
}

func TestSessionAdapterEnforcesDecoratedPromptLimitBeforeLaunch(t *testing.T) {
	request := func(prompt string) ExecuteRequest {
		return ExecuteRequest{
			CompletionMode: CompletionModeServerResponse,
			SessionID:      "session_decorated_prompt",
			AttemptID:      "attempt_decorated_prompt",
			RunID:          "run_1",
			NodeRunID:      "nr_1",
			WorkspacePath:  t.TempDir(),
			Prompt:         prompt,
			InvocationID:   "invocation_decorated_prompt",
			OccurredAt:     "2026-03-02T10:21:15Z",
		}
	}
	metadataBytes := len(promptWithProviderInvocationMetadata("", "attempt_decorated_prompt", "invocation_decorated_prompt"))
	exactPrompt := strings.Repeat("x", providerlimits.PromptBytes-metadataBytes-2)

	t.Run("exact decorated limit launches", func(t *testing.T) {
		runner := guardianfixture.NewOutputRunner(successfulProviderEvents("ok"), nil)
		adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
		if err != nil {
			t.Fatalf("new session adapter: %v", err)
		}
		if _, err := adapter.Execute(context.Background(), request(exactPrompt)); err != nil {
			t.Fatalf("exact decorated prompt boundary rejected: %v", err)
		}
		if runner.Starts() != 1 {
			t.Fatalf("exact decorated prompt launches=%d, want 1", runner.Starts())
		}
	})

	t.Run("one byte overflow does not launch", func(t *testing.T) {
		runner := guardianfixture.NewOutputRunner(nil, nil)
		adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
		if err != nil {
			t.Fatalf("new session adapter: %v", err)
		}
		_, err = adapter.Execute(context.Background(), request(exactPrompt+"x"))
		var limitErr *providerlimits.LimitExceededError
		if !errors.As(err, &limitErr) || limitErr.Resource != "prompt" {
			t.Fatalf("decorated prompt overflow error=%v, want typed prompt limit error", err)
		}
		if runner.Starts() != 0 {
			t.Fatalf("decorated prompt overflow reached provider launch %d times", runner.Starts())
		}
	})
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

func TestRunGuardedCommandCancelsOverflowBeforeDrainingContinuedOutput(t *testing.T) {
	cancelled := make(chan struct{})
	reader := &continuedOutputReader{remaining: providerlimits.GuardedCommandStreamBytes + 1, cancelled: cancelled, drained: make(chan struct{})}
	handle := &continuedOutputHandle{stdout: io.NopCloser(reader), stderr: io.NopCloser(strings.NewReader("")), cancelled: cancelled, stdoutDrained: reader.drained}
	runner := continuedOutputRunner{handle: handle}

	_, _, result, err := runGuardedCommand(context.Background(), runner, processguardian.Command{Path: "continued-output"})
	var limitErr *providerlimits.LimitExceededError
	if !errors.As(err, &limitErr) {
		t.Fatalf("continued output error=%v, want limit error", err)
	}
	if !handle.cancelObserved || !handle.drainObserved {
		t.Fatalf("overflow did not cancel before stream drain: cancel=%t drained=%t", handle.cancelObserved, handle.drainObserved)
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

type continuedOutputHandle struct {
	stdout         io.ReadCloser
	stderr         io.ReadCloser
	cancelled      chan struct{}
	stdoutDrained  <-chan struct{}
	cancelOnce     sync.Once
	cancelObserved bool
	drainObserved  bool
}

func (h *continuedOutputHandle) GuardID() string       { return "continued-output" }
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
	<-h.stdoutDrained
	h.drainObserved = true
	return processguardian.Result{Quiescent: true}, nil
}

type continuedOutputReader struct {
	remaining int64
	cancelled <-chan struct{}
	continued bool
	drained   chan struct{}
	once      sync.Once
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
	r.once.Do(func() { close(r.drained) })
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
