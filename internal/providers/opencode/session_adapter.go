package opencode

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/processguardian"
	"kingdom/internal/providerlimits"
)

type SessionAdapterConfig struct {
	BinaryPath              string
	WorkDir                 string
	ConfigDir               string
	DatabasePath            string
	InlineConfigContent     string
	Model                   string
	Variant                 string
	Agent                   string
	SkipPermissions         bool
	FirstEventTimeout       time.Duration
	StreamInactivityTimeout time.Duration
	Runner                  GuardianRunner
}

type SessionAdapter struct {
	mu              sync.Mutex
	closed          bool
	closeCh         chan struct{}
	streamWG        sync.WaitGroup
	closeWaitOnce   sync.Once
	closeWaitDone   chan struct{}
	helperWG        sync.WaitGroup
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	helperPaths     []string

	binaryPath              string
	workDir                 string
	configDir               string
	databasePath            string
	inlineConfigContent     string
	model                   string
	variant                 string
	agent                   string
	skipPermissions         bool
	firstEventTimeout       time.Duration
	streamInactivityTimeout time.Duration
	runner                  GuardianRunner

	logicalToActual map[string]string
	attempts        map[string]*sessionAttemptRecord
	inFlight        map[string]processguardian.Handle
	attemptCancels  map[string]context.CancelFunc
	attemptDone     map[string]chan struct{}
}

type opencodeRunEvent struct {
	activityContent       map[string]providerexecution.ActivityContent
	textContent           *eventTextContent
	Type                  string   `json:"type"`
	SessionID             string   `json:"sessionID"`
	ErrorText             string   `json:"-"`
	ProviderErrorCode     string   `json:"-"`
	ProviderErrorCategory string   `json:"-"`
	ProviderErrorStatus   int      `json:"-"`
	RetryAt               string   `json:"-"`
	RetryAfterMS          *float64 `json:"-"`
	Part                  struct {
		Type          string `json:"type"`
		ID            string `json:"id"`
		CallID        string `json:"callID"`
		MessageID     string `json:"messageID"`
		Text          string `json:"text"`
		Synthetic     bool   `json:"synthetic"`
		Tool          string `json:"tool"`
		Reason        string `json:"reason"`
		ReasonPresent bool   `json:"-"`
		State         struct {
			Status string          `json:"status"`
			Title  string          `json:"title"`
			Input  json.RawMessage `json:"input"`
		} `json:"state"`
		Metadata struct {
			OpenAI struct {
				ReasoningEncryptedContent string `json:"reasoningEncryptedContent"`
			} `json:"openai"`
			Redacted string `json:"redacted"`
		} `json:"metadata"`
		Time struct {
			Start *int64 `json:"start"`
			End   *int64 `json:"end"`
		} `json:"time"`
		Tokens *struct {
			Total     int64 `json:"total"`
			Input     int64 `json:"input"`
			Output    int64 `json:"output"`
			Reasoning int64 `json:"reasoning"`
			Cache     struct {
				Read  int64 `json:"read"`
				Write int64 `json:"write"`
			} `json:"cache"`
		} `json:"tokens,omitempty"`
	} `json:"part"`
}

type streamDiagnostics struct {
	ProcessStartedAt  string
	FirstEventAt      string
	LastEventAt       string
	ProcessExitedAt   string
	FirstEventLatency time.Duration
	MaxInterEventGap  time.Duration
	EventCount        int
}

type executionPathDiagnostics struct {
	AttemptID                string
	SessionID                string
	CmdStartedAt             string
	CmdWaitStartedAt         string
	CmdWaitFinishedAt        string
	WaitReturned             bool
	SucceededEventAppended   bool
	FailedEventAppended      bool
	CancelledEventAppended   bool
	StepFinishSeen           bool
	LastStepFinishReason     string
	LastStepFinishReasonKind string
	TerminalStateClass       string
	UpstreamErrorText        string
	CompletionGatePassed     bool
	StdoutEOFSeen            bool
	StderrEOFSeen            bool
	StdoutEventCount         int
	StdoutMalformedLineCount int
	StderrLineCount          int
	LastStdoutEventAt        string
	FirstStdoutMalformedAt   string
	LastStdoutMalformedAt    string
	FirstStdoutMalformedSHA  string
	LastStdoutMalformedSHA   string
	LastStderrLineAt         string
	LastProgressTextLen      int
	LastProgressTextSHA      string
	ContinuationTurnsElapsed int
	ExitCode                 *int
	ExitSignal               *int
	WaitError                string
}

type stderrCapture struct {
	raw          []byte
	lineCount    int
	lastLineAt   string
	eofSeen      bool
	scannerError string
}

type providerTimeoutError struct {
	TimeoutType string
	Timeout     time.Duration
}

const (
	providerInvocationMetadataHeader = "Runtime Invocation Metadata:"
	providerInvocationAttemptLabel   = "AttemptID:"
	providerInvocationIDLabel        = "KingdomProviderInvocationID:"
)

type IncompleteTerminalStateError struct {
	Reason string
}

type continuationTurnResult struct {
	TextPreview           string
	Tokens                TokenUsage
	ProviderSessionID     string
	UpstreamErrorText     string
	ProviderErrorPresent  bool
	ProviderErrorCode     string
	ProviderErrorCategory string
	ProviderErrorStatus   int
	RetryAt               string
	RetryAfterMS          *float64
	Stderr                stderrCapture
	WaitError             string
	EventCount            int
	MalformedCount        int
	MeaningfulProgress    bool
	ProcessExited         bool
	ExitCode              *int
	ExitSignal            *int
	StdoutDrained         bool
	StderrDrained         bool
	TimedOut              bool
	GuardResult           processguardian.Result
}

type startedOpencodeRun struct {
	handle                 processguardian.Handle
	stdout                 io.ReadCloser
	stderr                 io.ReadCloser
	releaseDatabaseStartup func()
	releaseStartupOnce     sync.Once
}

func (r *startedOpencodeRun) databaseStartupObserved() {
	if r == nil || r.releaseDatabaseStartup == nil {
		return
	}
	r.releaseStartupOnce.Do(r.releaseDatabaseStartup)
}

func (e *IncompleteTerminalStateError) Error() string {
	return fmt.Sprintf("incomplete terminal state (reason=%q)", strings.TrimSpace(e.Reason))
}

func (e *providerTimeoutError) Error() string {
	return fmt.Sprintf("provider %s exceeded after %s", strings.TrimSpace(e.TimeoutType), e.Timeout)
}

func executeResponseForCompletionResult(result attemptCompletionResult) ExecuteResponse {
	return ExecuteResponse{
		Accepted:          result.State == attemptCompletionStateSuccess,
		OutputText:        strings.TrimSpace(result.OutputText),
		Tokens:            result.Tokens,
		ProviderSessionID: strings.TrimSpace(result.ProviderSessionID),
		Outcome:           executionOutcomeFromAttemptCompletionResult(result),
	}
}

func withExecuteProviderDiagnostics(resp ExecuteResponse, diagnosticsJSON string) ExecuteResponse {
	resp.ProviderDiagnosticsJSON = firstNonEmpty(strings.TrimSpace(diagnosticsJSON), "{}")
	return resp
}

func withInvocationStreamObservation(resp ExecuteResponse, observation InvocationStreamObservation) ExecuteResponse {
	resp.StreamObservation = observation
	return resp
}

func executeErrorForCompletionResult(result attemptCompletionResult) error {
	switch result.State {
	case attemptCompletionStateCancelled:
		return context.Canceled
	case attemptCompletionStateIncompleteTurn:
		reason := firstNonEmpty(strings.TrimSpace(result.ReasonCode), strings.TrimSpace(result.FailureText), "provider_incomplete_turn")
		return fmt.Errorf("opencode run failed: %w", &IncompleteTerminalStateError{Reason: reason})
	case attemptCompletionStateFailed:
		failureText := firstNonEmpty(strings.TrimSpace(result.FailureText), strings.TrimSpace(result.ReasonCode), "opencode run failed")
		return fmt.Errorf("opencode run failed: %s", failureText)
	default:
		return nil
	}
}

func executionResultForTimeout(timeoutErr error, providerSessionID string) attemptCompletionResult {
	return attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
		ProviderSessionID: strings.TrimSpace(providerSessionID),
		Timeout:           true,
		FailureText:       firstNonEmpty(strings.TrimSpace(errorString(timeoutErr)), "opencode run timed out"),
	}).Result
}

func adjudicateFatalExecutionEvidence(evidence attemptCompletionEvidence) (attemptCompletionResult, bool) {
	fatal := evidence.Cancelled ||
		evidence.Timeout ||
		evidence.ProviderErrorPresent ||
		strings.TrimSpace(evidence.UpstreamErrorText) != "" ||
		evidence.ProcessFailed ||
		evidence.MalformedProtocol ||
		evidence.ConflictingTerminalEvidence ||
		evidence.LaterEventAfterTerminal ||
		(evidence.StepFinishSeen && evidence.LastStepFinish.Class == stepFinishTerminalFailure)
	if !fatal {
		return attemptCompletionResult{}, false
	}
	return attemptCompletionAdjudicator{}.Adjudicate(evidence).Result, true
}

func providerProcessObservationFailed(processExited bool, exitCode, exitSignal *int, stdoutDrained, stderrDrained bool) bool {
	return !processExited ||
		exitCode == nil ||
		*exitCode != 0 ||
		(exitSignal != nil && *exitSignal != 0) ||
		!stdoutDrained ||
		!stderrDrained
}

func executionResultForProcessError(err error, providerSessionID, upstreamErrorText string, providerErrorPresent bool, providerErrorCode, providerErrorCategory string, providerErrorStatus int, failureText string) attemptCompletionResult {
	return attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
		ProviderSessionID:     strings.TrimSpace(providerSessionID),
		UpstreamErrorText:     strings.TrimSpace(upstreamErrorText),
		ProviderErrorPresent:  providerErrorPresent,
		ProviderErrorCode:     strings.TrimSpace(providerErrorCode),
		ProviderErrorCategory: strings.TrimSpace(providerErrorCategory),
		ProviderErrorStatus:   providerErrorStatus,
		ProcessFailed:         true,
		FailureText:           firstNonEmpty(strings.TrimSpace(failureText), strings.TrimSpace(errorString(err)), "opencode run failed"),
	}).Result
}

func executionResultForCancellation(err error, providerSessionID, failureText string) attemptCompletionResult {
	return attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
		ProviderSessionID: strings.TrimSpace(providerSessionID),
		Cancelled:         true,
		FailureText:       firstNonEmpty(strings.TrimSpace(failureText), strings.TrimSpace(errorString(err)), "opencode run cancelled"),
	}).Result
}

func shouldContinueLiveStream(reason stepFinishReasonEvidence) bool {
	switch reason.Normalized {
	case "tool-calls", "unknown":
		return true
	default:
		return false
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func accumulateTokenUsage(total *TokenUsage, delta TokenUsage) {
	if total == nil {
		return
	}
	total.InputTokens += delta.InputTokens
	total.OutputTokens += delta.OutputTokens
	total.TotalTokens += delta.TotalTokens
	total.ReasoningTokens += delta.ReasoningTokens
	total.CacheReadTokens += delta.CacheReadTokens
	total.CacheWriteTokens += delta.CacheWriteTokens
}

// WarmRuntime resolves this adapter's agent against the real managed OpenCode
// database and config directory, so the work OpenCode does exactly once per
// installation is done before an operator is waiting on it.
//
// The first `opencode run` against a fresh workspace is the moment OpenCode
// creates and migrates its database, loads the config overlay, and resolves the
// agent profile — seconds of startup charged to whoever sent the first planner
// message. Strict startup validation cannot pay it, because that deliberately
// runs against a throwaway database copy so validation can never write to
// Kingdom's own. `debug agent` resolves configuration and opens the database
// without creating a session, which is precisely the warm needed and nothing
// more.
//
// Taking the database startup gate matters: this opens the same database an
// execution run might be opening, and the gate is what keeps two processes off
// the WAL pragma at once.
func (a *SessionAdapter) WarmRuntime(ctx context.Context) error {
	agent := strings.TrimSpace(a.agent)
	if agent == "" {
		return nil
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return fmt.Errorf("provider adapter is closed")
	}
	a.helperWG.Add(1)
	a.mu.Unlock()
	defer a.helperWG.Done()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(a.lifecycleCtx, cancel)
	defer func() { stop(); cancel() }()
	release, err := opencodeDatabaseStartupGates.acquire(ctx, a.databasePath)
	if err != nil {
		return fmt.Errorf("wait for opencode database startup: %w", err)
	}
	defer release()
	commandEnv, err := opencodeCommandEnvironment(ExecuteRequest{}, a.configDir, a.databasePath, a.inlineConfigContent)
	if err != nil {
		return fmt.Errorf("build opencode warmup environment: %w", err)
	}
	stdout, stderr, _, err := runGuardedCommand(ctx, a.runner, processguardian.Command{
		Path: a.binaryPath,
		Args: []string{"debug", "agent", agent},
		Dir:  a.workDir,
		Env:  opencodeProcessEnvironment(commandEnv),
	})
	a.retainHelperPaths(stdout.Path, stderr.Path)
	if err != nil {
		return fmt.Errorf("resolve opencode agent %q: %w: %s (stdout %s, stderr %s)", agent, err, strings.TrimSpace(string(append(stdout.Preview, stderr.Preview...))), stdout.Path, stderr.Path)
	}
	return nil
}

func (a *SessionAdapter) startOpencodeRun(ctx context.Context, req ExecuteRequest, sessionID string, commandOrdinal int) (startedOpencodeRun, error) {
	// `--thinking` makes `opencode run --format json` emit `reasoning` parts on
	// the event stream. Without it the stream carries only text/tool/step parts
	// and reasoning is reachable only via a post-hoc `opencode export`, so the
	// planner can never surface live "thinking". For non-reasoning models the
	// flag is a no-op (no reasoning parts exist to print).
	args := []string{"run", "--format", "json", "--thinking"}
	if a.skipPermissions {
		args = append(args, "--dangerously-skip-permissions")
	}
	if strings.TrimSpace(sessionID) != "" {
		args = append(args, "--session", strings.TrimSpace(sessionID))
	}
	model := strings.TrimSpace(req.Model)
	if model == "" {
		model = a.model
	}
	if model != "" {
		args = append(args, "--model", model)
	}
	variant := strings.TrimSpace(req.Variant)
	if variant == "" {
		variant = a.variant
	}
	if variant != "" {
		args = append(args, "--variant", variant)
	}
	agent := strings.TrimSpace(req.Agent)
	if agent == "" {
		agent = a.agent
	}
	if agent != "" {
		args = append(args, "--agent", agent)
	}
	if strings.TrimSpace(req.WorkspacePath) != "" {
		args = append(args, "--dir", strings.TrimSpace(req.WorkspacePath))
	}
	dir := a.workDir
	if strings.TrimSpace(req.WorkspacePath) != "" {
		dir = strings.TrimSpace(req.WorkspacePath)
	}
	databasePath := effectiveDatabasePath(req.DatabasePath, a.databasePath)
	commandEnv, err := opencodeCommandEnvironment(req, a.configDir, databasePath, a.inlineConfigContent)
	if err != nil {
		return startedOpencodeRun{}, fmt.Errorf("build opencode run environment: %w", err)
	}
	command := processguardian.Command{Path: a.binaryPath, Args: args, Dir: dir, Env: opencodeProcessEnvironment(commandEnv)}
	releaseStartup, err := opencodeDatabaseStartupGates.acquire(ctx, databasePath)
	if err != nil {
		return startedOpencodeRun{}, fmt.Errorf("wait for opencode database startup: %w", err)
	}
	startedAt := time.Now().UTC().Format(time.RFC3339Nano)
	var (
		acknowledgedAt     string
		acknowledgedAtOnce sync.Once
	)
	var persist processguardian.PersistOwnership
	if req.OnProcessGuardOwnership != nil {
		persist = func(callbackCtx context.Context, ownership processguardian.Ownership) error {
			acknowledgedAtOnce.Do(func() { acknowledgedAt = time.Now().UTC().Format(time.RFC3339Nano) })
			return req.OnProcessGuardOwnership(callbackCtx, ProcessGuardOwnership{CommandOrdinal: commandOrdinal, StartedAt: startedAt, AcknowledgedAt: acknowledgedAt, Ownership: ownership})
		}
	}
	handle, err := a.runner.Start(ctx, command, persist)
	if err != nil {
		releaseStartup()
		var launchErr *processguardian.LaunchError
		if errors.As(err, &launchErr) && launchErr.OwnershipPersisted && req.OnProcessGuardQuiescence != nil {
			if callbackErr := req.OnProcessGuardQuiescence(context.Background(), ProcessGuardQuiescence{CommandOrdinal: commandOrdinal, ExitedAt: time.Now().UTC().Format(time.RFC3339Nano), QuiescentAt: time.Now().UTC().Format(time.RFC3339Nano), Result: launchErr.Result}); callbackErr != nil {
				err = errors.Join(err, fmt.Errorf("record aborted process guard quiescence: %w", callbackErr))
			}
		}
		return startedOpencodeRun{}, fmt.Errorf("start guarded opencode run: %w", err)
	}
	return startedOpencodeRun{
		handle:                 handle,
		stdout:                 handle.Stdout(),
		stderr:                 handle.Stderr(),
		releaseDatabaseStartup: releaseStartup,
	}, nil
}

// writeOpenCodePrompt streams each prompt part to OpenCode's stdin. The timeout
// is an inactivity timeout that resets only when bytes are delivered; it never
// places a total byte or duration ceiling on a progressing prompt.
func writeOpenCodePrompt(ctx context.Context, handle processguardian.Handle, parts []string, inactivityTimeout time.Duration, producers ...func(context.Context, io.Writer) error) error {
	if handle == nil {
		return fmt.Errorf("OpenCode process handle is unavailable")
	}
	stdin := handle.Stdin()
	if stdin == nil {
		return fmt.Errorf("OpenCode stdin is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancelInput := context.WithCancel(ctx)
	defer cancelInput()
	write := func(note func()) error {
		if len(producers) == 0 {
			return writeOpenCodePromptChunks(stdin, parts, note)
		}
		err := producers[0](ctx, &promptProgressWriter{ctx: ctx, writer: stdin, progress: note})
		return errors.Join(err, stdin.Close())
	}
	if inactivityTimeout <= 0 {
		return write(nil)
	}
	progress := make(chan struct{}, 1)
	done := make(chan error, 1)
	var progressMu sync.Mutex
	lastProgressAt := time.Now()
	noteProgress := func() {
		progressMu.Lock()
		lastProgressAt = time.Now()
		progressMu.Unlock()
		select {
		case progress <- struct{}{}:
		default:
		}
	}
	go func() { done <- write(noteProgress) }()
	timer := time.NewTimer(inactivityTimeout)
	defer timer.Stop()
	resetTimer := func() {
		progressMu.Lock()
		remaining := inactivityTimeout - time.Since(lastProgressAt)
		progressMu.Unlock()
		if remaining < 0 {
			remaining = 0
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(remaining)
	}
	for {
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			cancelErr := handle.Cancel(context.Background())
			return errors.Join(ctx.Err(), cancelErr, <-done)
		case <-progress:
			resetTimer()
		case <-timer.C:
			select {
			case err := <-done:
				return err
			case <-progress:
				resetTimer()
				continue
			default:
			}
			progressMu.Lock()
			stalled := time.Since(lastProgressAt) >= inactivityTimeout
			progressMu.Unlock()
			if !stalled {
				resetTimer()
				continue
			}
			timeoutErr := &providerTimeoutError{TimeoutType: "prompt_input_stalled", Timeout: inactivityTimeout}
			cancelInput()
			cancelErr := handle.Cancel(context.Background())
			return errors.Join(timeoutErr, cancelErr, <-done)
		}
	}
}

func writeOpenCodePromptChunks(stdin io.WriteCloser, parts []string, onProgress func()) error {
	// This is only an I/O copy buffer; the complete prompt is never capped to
	// this size or accumulated into another contiguous byte slice.
	const writeChunkBytes = 64 * 1024
	buffer := make([]byte, writeChunkBytes)
	var writeErr error
	for _, part := range parts {
		for offset := 0; offset < len(part); {
			end := min(offset+len(buffer), len(part))
			chunk := buffer[:copy(buffer, part[offset:end])]
			writtenInChunk := 0
			for writtenInChunk < len(chunk) {
				written, err := stdin.Write(chunk[writtenInChunk:])
				if written > 0 {
					writtenInChunk += written
					if onProgress != nil {
						onProgress()
					}
				}
				if err != nil {
					writeErr = fmt.Errorf("write OpenCode prompt to stdin: %w", err)
					break
				}
				if written == 0 {
					writeErr = fmt.Errorf("write OpenCode prompt to stdin: %w", io.ErrNoProgress)
					break
				}
			}
			if writeErr != nil {
				break
			}
			offset = end
		}
		if writeErr != nil {
			break
		}
	}
	closeErr := stdin.Close()
	if writeErr != nil {
		if closeErr != nil {
			writeErr = errors.Join(writeErr, fmt.Errorf("close OpenCode stdin: %w", closeErr))
		}
		return writeErr
	}
	if closeErr != nil {
		return fmt.Errorf("close OpenCode stdin after prompt: %w", closeErr)
	}
	return nil
}

func readStderrCapture(stream io.Reader) stderrCapture {
	capture := stderrCapture{}
	buffer := make([]byte, 32*1024)
	lineOpen := false
	for {
		count, err := stream.Read(buffer)
		if count > 0 {
			chunk := buffer[:count]
			capture.raw = appendStderrTail(capture.raw, chunk)
			newlineCount := bytes.Count(chunk, []byte{'\n'})
			if newlineCount > 0 {
				capture.lineCount += newlineCount
				capture.lastLineAt = time.Now().UTC().Format(time.RFC3339)
			}
			lineOpen = chunk[len(chunk)-1] != '\n'
		}
		if err != nil {
			if err != io.EOF {
				capture.scannerError = err.Error()
			}
			break
		}
	}
	if lineOpen {
		capture.lineCount++
		capture.lastLineAt = time.Now().UTC().Format(time.RFC3339)
	}
	capture.eofSeen = capture.scannerError == ""
	return capture
}

func appendStderrTail(existing, next []byte) []byte {
	limit := providerlimits.StderrTailBytes
	if len(next) >= limit {
		return append(existing[:0], next[len(next)-limit:]...)
	}
	if len(existing)+len(next) <= limit {
		return append(existing, next...)
	}
	keep := limit - len(next)
	copy(existing, existing[len(existing)-keep:])
	return append(existing[:keep], next...)
}

func startStderrCapture(stream io.Reader) <-chan stderrCapture {
	done := make(chan stderrCapture, 1)
	go func() {
		done <- readStderrCapture(stream)
	}()
	return done
}

type guardedCommandReadResult struct {
	raw capturedOutput
	err error
}

func runGuardedCommand(ctx context.Context, runner GuardianRunner, command processguardian.Command) (capturedOutput, capturedOutput, processguardian.Result, error) {
	return runGuardedCommandWithLifecycle(ctx, runner, command, false, 0)
}

// runBoundedGuardedCommand keeps every command-lifecycle phase within ctx's
// deadline. Unlike provider execution, diagnostic callers may not wait beyond
// their observation budget for quiescence or a blocked pipe drain.
func runBoundedGuardedCommand(ctx context.Context, runner GuardianRunner, command processguardian.Command, cancelReserve time.Duration) (capturedOutput, capturedOutput, processguardian.Result, error) {
	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		return capturedOutput{}, capturedOutput{}, processguardian.Result{}, fmt.Errorf("bounded guarded command requires a deadline")
	}
	if cancelReserve < 0 {
		cancelReserve = 0
	}
	return runGuardedCommandWithLifecycle(ctx, runner, command, true, time.Until(deadline.Add(-cancelReserve)))
}

func runGuardedCommandWithLifecycle(ctx context.Context, runner GuardianRunner, command processguardian.Command, bounded bool, cancelAfter time.Duration) (capturedOutput, capturedOutput, processguardian.Result, error) {
	if runner == nil {
		var err error
		runner, err = processguardian.NewRunner(processguardian.Config{})
		if err != nil {
			return capturedOutput{}, capturedOutput{}, processguardian.Result{}, err
		}
	}
	handle, err := runner.Start(ctx, command, nil)
	if err != nil {
		return capturedOutput{}, capturedOutput{}, processguardian.Result{}, err
	}
	return collectGuardedCommandWithLifecycle(ctx, handle, bounded, cancelAfter)
}

func collectGuardedCommandWithLifecycle(ctx context.Context, handle processguardian.Handle, bounded bool, cancelAfter time.Duration) (capturedOutput, capturedOutput, processguardian.Result, error) {
	stdoutDone := make(chan guardedCommandReadResult, 1)
	stderrDone := make(chan guardedCommandReadResult, 1)
	stdoutStream, stderrStream := handle.Stdout(), handle.Stderr()
	var cancelCtx context.Context
	var cancelCtxDone context.CancelFunc
	if bounded {
		deadline, _ := ctx.Deadline()
		cancelCtx, cancelCtxDone = context.WithDeadline(context.Background(), deadline)
		defer cancelCtxDone()
	}
	var cancelOnce sync.Once
	cancel := func() {
		cancelOnce.Do(func() {
			if bounded {
				_ = handle.Cancel(cancelCtx)
				return
			}
			_ = handle.Cancel(context.Background())
		})
	}
	stdinErr := handle.Stdin().Close()
	if stdinErr != nil {
		cancel()
	}
	go func() {
		defer stdoutStream.Close()
		raw, readErr := captureCommandOutput(stdoutStream)
		if readErr != nil {
			_ = stdoutStream.Close()
			cancel()
		}
		stdoutDone <- guardedCommandReadResult{raw: raw, err: readErr}
	}()
	go func() {
		defer stderrStream.Close()
		raw, readErr := captureCommandOutput(stderrStream)
		if readErr != nil {
			_ = stderrStream.Close()
			cancel()
		}
		stderrDone <- guardedCommandReadResult{raw: raw, err: readErr}
	}()
	cancelDone := make(chan struct{})
	defer close(cancelDone)
	if bounded {
		if cancelAfter <= 0 {
			cancel()
		} else {
			go func() {
				timer := time.NewTimer(cancelAfter)
				defer timer.Stop()
				select {
				case <-timer.C:
					cancel()
				case <-ctx.Done():
					cancel()
				case <-cancelDone:
				}
			}()
		}
	}
	if !bounded {
		go func() {
			select {
			case <-ctx.Done():
				cancel()
			case <-cancelDone:
			}
		}()
	}
	waitCtx := context.Background()
	if bounded {
		waitCtx = ctx
	}
	result, waitErr := handle.Wait(waitCtx)
	if bounded && waitErr != nil {
		cancel()
		_ = stdoutStream.Close()
		_ = stderrStream.Close()
	}
	stdout, stdoutComplete := awaitGuardedCommandRead(ctx, stdoutDone, bounded)
	stderr, stderrComplete := awaitGuardedCommandRead(ctx, stderrDone, bounded)
	if bounded && (!stdoutComplete || !stderrComplete) {
		cancel()
		_ = stdoutStream.Close()
		_ = stderrStream.Close()
		if !stdoutComplete {
			go retireLateCommandCapture(stdoutDone)
		}
		if !stderrComplete {
			go retireLateCommandCapture(stderrDone)
		}
		return stdout.raw, stderr.raw, processguardian.Result{}, firstNonNilError(stdout.err, stderr.err, ctx.Err(), fmt.Errorf("diagnostic command stream drain did not complete before deadline"))
	}
	return stdout.raw, stderr.raw, result, firstNonNilError(stdout.err, stderr.err, stdinErr, waitErr, guardianResultError(result))
}

func retireLateCommandCapture(done <-chan guardedCommandReadResult) {
	result := <-done
	if result.raw.Path != "" {
		_ = os.Remove(result.raw.Path)
	}
}

func awaitGuardedCommandRead(ctx context.Context, done <-chan guardedCommandReadResult, bounded bool) (guardedCommandReadResult, bool) {
	if !bounded {
		return <-done, true
	}
	select {
	case result := <-done:
		return result, true
	case <-ctx.Done():
		select {
		case result := <-done:
			return result, true
		default:
			return guardedCommandReadResult{}, false
		}
	}
}

// awaitGuardedRun deliberately waits without the request context: cancellation
// has already been relayed to the guardian, and returning before its quiescence
// proof or either stream drain would leak an unaccounted provider command tree.
func awaitGuardedRun(req ExecuteRequest, commandOrdinal int, handle processguardian.Handle, stderrDone <-chan stderrCapture) (processguardian.Result, stderrCapture, error) {
	result, waitErr := handle.Wait(context.Background())
	stderr := <-stderrDone
	if req.OnProcessGuardQuiescence != nil {
		callbackErr := req.OnProcessGuardQuiescence(context.Background(), ProcessGuardQuiescence{
			CommandOrdinal: commandOrdinal,
			ExitedAt:       time.Now().UTC().Format(time.RFC3339Nano),
			QuiescentAt:    time.Now().UTC().Format(time.RFC3339Nano),
			Result:         result,
		})
		if callbackErr != nil {
			waitErr = firstNonNilError(waitErr, fmt.Errorf("record process guard quiescence: %w", callbackErr))
		}
	}
	return result, stderr, firstNonNilError(waitErr, guardianResultError(result))
}

func guardianResultError(result processguardian.Result) error {
	if !result.Quiescent {
		return fmt.Errorf("guardian did not prove provider process quiescence")
	}
	if strings.TrimSpace(result.FailureCode) != "" {
		return fmt.Errorf("guardian provider failure (%s): %s", result.FailureCode, strings.TrimSpace(result.FailureText))
	}
	if result.ExitCode != 0 || result.ExitSignal != 0 {
		return fmt.Errorf("guarded provider exited with code %d signal %d", result.ExitCode, result.ExitSignal)
	}
	return nil
}

func processGuardObservation(commandOrdinal int, result processguardian.Result) ProcessGuardObservation {
	return ProcessGuardObservation{
		CommandOrdinal: commandOrdinal,
		GuardID:        strings.TrimSpace(result.GuardID),
		GuardianPID:    result.GuardianPID,
		ChildPID:       result.ChildPID,
		TreeID:         result.TreeID,
		Platform:       strings.TrimSpace(result.Platform),
		Quiescent:      result.Quiescent,
		Forced:         result.Forced,
		EvidenceSHA256: strings.TrimSpace(result.EvidenceSHA256),
		FailureCode:    strings.TrimSpace(result.FailureCode),
		FailureText:    strings.TrimSpace(result.FailureText),
	}
}

func mergeStderrCapture(current, next stderrCapture) stderrCapture {
	if len(next.raw) > 0 {
		current.raw = appendStderrTail(current.raw, next.raw)
	}
	current.lineCount += next.lineCount
	current.lastLineAt = firstNonEmpty(strings.TrimSpace(next.lastLineAt), strings.TrimSpace(current.lastLineAt))
	current.eofSeen = current.eofSeen && next.eofSeen
	if strings.TrimSpace(next.scannerError) != "" {
		if strings.TrimSpace(current.scannerError) == "" {
			current.scannerError = next.scannerError
		} else {
			current.scannerError += "; " + next.scannerError
		}
	}
	return current
}

func opencodeProcessEnvironment(explicitEnv ...[]string) []string {
	base := os.Environ()
	overrides := make(map[string]struct{})
	for _, entries := range explicitEnv {
		for _, entry := range entries {
			name, _, _ := strings.Cut(entry, "=")
			overrides[strings.ToUpper(name)] = struct{}{}
		}
	}
	env := make([]string, 0, len(base))
	for _, entry := range base {
		name, _, _ := strings.Cut(entry, "=")
		upperName := strings.ToUpper(name)
		if _, replaced := overrides[upperName]; replaced {
			continue
		}
		env = append(env, entry)
	}
	for _, entries := range explicitEnv {
		env = append(env, entries...)
	}
	return env
}

func ConfigDirEnvironment(configDir string) []string {
	trimmed := strings.TrimSpace(configDir)
	if trimmed == "" {
		return nil
	}
	return []string{"OPENCODE_CONFIG_DIR=" + trimmed}
}

func DatabasePathEnvironment(databasePath string) []string {
	trimmed := strings.TrimSpace(databasePath)
	if trimmed == "" {
		return nil
	}
	return []string{"OPENCODE_DB=" + trimmed}
}

func configContentEnvironment(configContent string) []string {
	trimmed := strings.TrimSpace(configContent)
	if trimmed == "" {
		return nil
	}
	return []string{"OPENCODE_CONFIG_CONTENT=" + trimmed}
}

func effectiveDatabasePath(requestDatabasePath, defaultDatabasePath string) string {
	databasePath := strings.TrimSpace(requestDatabasePath)
	if databasePath == "" {
		databasePath = strings.TrimSpace(defaultDatabasePath)
	}
	return databasePath
}

func opencodeCommandEnvironment(req ExecuteRequest, defaultConfigDir string, databasePath string, defaultInlineConfigContent string) ([]string, error) {
	var env []string
	if projectRootBoundary := strings.TrimSpace(req.ProjectRootBoundary); projectRootBoundary != "" {
		boundary, err := filepath.Abs(filepath.Clean(projectRootBoundary))
		if err != nil {
			return nil, fmt.Errorf("resolve OpenCode project root boundary: %w", err)
		}
		boundary, err = filepath.EvalSymlinks(boundary)
		if err != nil {
			return nil, fmt.Errorf("resolve OpenCode project root boundary: %w", err)
		}
		env = append(env, "GIT_CEILING_DIRECTORIES="+filepath.Dir(boundary))
	}
	configDir := strings.TrimSpace(req.ConfigDir)
	if configDir == "" {
		configDir = strings.TrimSpace(defaultConfigDir)
	}
	env = append(env, ConfigDirEnvironment(configDir)...)
	env = append(env, DatabasePathEnvironment(databasePath)...)
	inlineConfigContent := strings.TrimSpace(req.InlineConfigContent)
	if inlineConfigContent == "" {
		inlineConfigContent = defaultInlineConfigContent
	}
	return append(env, configContentEnvironment(inlineConfigContent)...), nil
}

func NewSessionAdapter(cfg SessionAdapterConfig) (*SessionAdapter, error) {
	if cfg.Runner == nil {
		runner, err := processguardian.NewRunner(processguardian.Config{CancelGrace: 500 * time.Millisecond, CancelForce: 500 * time.Millisecond})
		if err != nil {
			return nil, err
		}
		cfg.Runner = runner
	}
	binaryPath := strings.TrimSpace(cfg.BinaryPath)
	if binaryPath == "" {
		binaryPath = "opencode"
	}
	if _, err := exec.LookPath(binaryPath); err != nil {
		return nil, fmt.Errorf("resolve opencode binary %q: %w", binaryPath, err)
	}
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	return &SessionAdapter{
		lifecycleCtx: lifecycleCtx, lifecycleCancel: lifecycleCancel,
		closeCh:                 make(chan struct{}),
		binaryPath:              binaryPath,
		workDir:                 strings.TrimSpace(cfg.WorkDir),
		configDir:               strings.TrimSpace(cfg.ConfigDir),
		databasePath:            strings.TrimSpace(cfg.DatabasePath),
		inlineConfigContent:     strings.TrimSpace(cfg.InlineConfigContent),
		model:                   strings.TrimSpace(cfg.Model),
		variant:                 strings.TrimSpace(cfg.Variant),
		agent:                   strings.TrimSpace(cfg.Agent),
		skipPermissions:         cfg.SkipPermissions,
		firstEventTimeout:       cfg.FirstEventTimeout,
		streamInactivityTimeout: cfg.StreamInactivityTimeout,
		logicalToActual:         make(map[string]string),
		attempts:                make(map[string]*sessionAttemptRecord),
		runner:                  cfg.Runner,
		inFlight:                make(map[string]processguardian.Handle),
		attemptCancels:          make(map[string]context.CancelFunc),
		attemptDone:             make(map[string]chan struct{}),
	}, nil
}

func (a *SessionAdapter) Execute(ctx context.Context, req ExecuteRequest) (response ExecuteResponse, executeErr error) {
	defer func() {
		if executeErr == nil && response.Outcome.State == "failed" {
			executeErr = fmt.Errorf("opencode run failed: %s", response.Outcome.FailureText)
		}
	}()
	if err := validateExecuteRequest(req); err != nil {
		return ExecuteResponse{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	invocationID := resolveProviderInvocationID(req)
	attemptCtx, cancelAttempt := context.WithCancel(ctx)
	defer cancelAttempt()
	attemptDone, attemptRecord, err := a.beginAttemptOperation(req.AttemptID)
	if err != nil {
		return ExecuteResponse{}, err
	}
	defer a.endAttemptOperation(req.AttemptID, attemptDone)
	releaseContent := func() error { return a.releaseAttempt(req.AttemptID, attemptRecord) }
	defer func() {
		if response.StreamObservation.ReleaseContent == nil {
			response.StreamObservation.ReleaseContent = releaseContent
		}
	}()
	a.setAttemptCancel(req.AttemptID, cancelAttempt)
	defer a.clearAttemptCancel(req.AttemptID)
	capture, err := newInvocationCapture()
	if err != nil {
		return ExecuteResponse{}, err
	}
	defer capture.finish(invocationID)
	a.retainContentPath(req.AttemptID, capture.stdout.output.Path)
	a.retainContentPath(req.AttemptID, capture.stderr.output.Path)
	tools, err := newToolStateStorage()
	if err != nil {
		return ExecuteResponse{}, err
	}
	defer tools.close()

	a.appendEvent(req.AttemptID, AttemptStreamEvent{
		AttemptID:   req.AttemptID,
		EventType:   AttemptEventTypeStarted,
		PayloadJSON: `{}`,
		OccurredAt:  req.OccurredAt,
	})
	if err := a.eventJournalError(req.AttemptID); err != nil {
		return ExecuteResponse{}, err
	}

	startAt := time.Time{}
	lastEventAt := time.Time{}
	diagnostics := streamDiagnostics{}

	actualSessionID := ""
	providerSessionObserved := false
	if !req.ForceNewSession {
		actualSessionID = firstNonEmpty(strings.TrimSpace(req.ProviderSessionID), a.actualSessionID(req.SessionID))
	}
	execDiag := executionPathDiagnostics{
		AttemptID: strings.TrimSpace(req.AttemptID),
		SessionID: strings.TrimSpace(req.SessionID),
	}
	startedRun, err := a.startOpencodeRun(attemptCtx, req, actualSessionID, 1)
	if err != nil {
		return ExecuteResponse{}, err
	}
	defer startedRun.databaseStartupObserved()
	stdout := startedRun.stdout
	stderr := startedRun.stderr
	defer stdout.Close()
	defer stderr.Close()
	startAt = time.Now().UTC()
	a.setInFlight(req.AttemptID, startedRun.handle)
	if req.OnProcessStarted != nil {
		req.OnProcessStarted()
	}
	diagnostics.ProcessStartedAt = startAt.Format(time.RFC3339)
	execDiag.CmdStartedAt = diagnostics.ProcessStartedAt

	ctxCancelDone := make(chan struct{})
	go func() {
		select {
		case <-attemptCtx.Done():
			_ = startedRun.handle.Cancel(context.Background())
		case <-ctxCancelDone:
		}
	}()
	defer close(ctxCancelDone)

	stderrDone := startStderrCapture(io.TeeReader(stderr, capture.stderr))

	textChunks := textSpool{}
	defer textChunks.close()
	responseState := responseStream{}
	responseState.resetStep()
	defer responseState.close()
	var accumulatedTokens TokenUsage
	stderrSnapshot := stderrCapture{}
	upstreamErrorText := ""
	providerErrorSeen := false
	providerErrorCode := ""
	providerErrorCategory := ""
	providerErrorStatus := 0
	providerRetryAt := ""
	var providerRetryAfterMS *float64
	meaningfulProgress := false
	allProcessesExited := false
	allStdoutDrained := false
	allStderrDrained := false
	continuationTimedOut := false
	inputTimedOut := false
	inputCancelled := false
	inputTerminalSet := false
	var watchdog *commandWatchdog
	processGuards := make([]ProcessGuardObservation, 0, 1)
	finish := func(resp ExecuteResponse) ExecuteResponse {
		canonicalTerminalContent, contentErr := responseState.terminalContent(invocationID)
		if canonicalTerminalContent != nil {
			a.retainContentPath(req.AttemptID, canonicalTerminalContent.Path)
		}
		staged, captureErr := capture.finish(invocationID)
		a.mu.Lock()
		journalErr := a.attempts[req.AttemptID].err
		a.mu.Unlock()
		toolPreview, pendingTools := tools.preview(), tools.pending()
		if err := firstNonNilError(contentErr, captureErr, journalErr, responseState.err, responseState.terminal.err, tools.err); err != nil {
			resp.Accepted = false
			resp.Outcome = ExecutionOutcome{State: "failed", ReasonCode: "provider_unknown_failure", FailureText: fmt.Sprintf("persist provider stream: %v", err), EvidenceSource: "stream"}
			providerErrorSeen = true
			upstreamErrorText = resp.Outcome.FailureText
		}
		terminalText := strings.TrimSpace(responseState.terminalOutput())
		timedOut := inputTimedOut || continuationTimedOut
		cancelled := inputCancelled
		if !inputTerminalSet {
			timedOut = timedOut || isExecutionTimedOut(attemptCtx.Err())
			cancelled = cancelled || isExecutionCancelled(attemptCtx.Err())
		}
		if watchdog != nil && watchdog.Err() != nil {
			timedOut = true
		}
		observation := InvocationStreamObservation{
			ReleaseContent:              releaseContent,
			StagedDiagnosticContent:     staged,
			ProviderSessionID:           strings.TrimSpace(actualSessionID),
			FinalTurnID:                 responseState.completed.turnID,
			FinalStepID:                 responseState.completed.stepID,
			FinalSequence:               responseState.completed.sequence,
			RawFinishReason:             responseState.completed.reason.Raw,
			NormalizedFinishReason:      strings.TrimSpace(responseState.completed.reason.Normalized),
			CanonicalTerminalText:       terminalText,
			ToolCalls:                   toolPreview,
			PendingToolCount:            pendingTools,
			Tokens:                      accumulatedTokens,
			ProcessExited:               allProcessesExited,
			ProcessExitCode:             execDiag.ExitCode,
			ProcessExitSignal:           execDiag.ExitSignal,
			StdoutDrained:               allStdoutDrained,
			StderrDrained:               allStderrDrained,
			ProcessQuiescent:            allProcessesExited && len(processGuards) > 0,
			ProcessGuards:               append([]ProcessGuardObservation(nil), processGuards...),
			TimedOut:                    timedOut,
			Cancelled:                   cancelled,
			ProviderErrorPresent:        providerErrorSeen,
			ProviderError:               strings.TrimSpace(upstreamErrorText),
			ProviderErrorCode:           strings.TrimSpace(providerErrorCode),
			ProviderErrorCategory:       strings.TrimSpace(providerErrorCategory),
			ProviderErrorStatus:         providerErrorStatus,
			MalformedProtocolCount:      execDiag.StdoutMalformedLineCount,
			ConflictingTerminalEvidence: responseState.conflictingTerminalEvidence,
			LaterEventAfterTerminal:     responseState.laterEventAfterTerminal(),
			MeaningfulProgress:          meaningfulProgress || accumulatedTokens.ReasoningTokens > 0 || accumulatedTokens.OutputTokens > 0,
		}
		if content := canonicalTerminalContent; content != nil {
			observation.CanonicalTerminalTextRef = &content.Reference
			observation.StagedTerminalContent = content
			observation.TerminalTextOpener = content.Open
			if content.Reference.SizeBytes > streamPreviewBytes {
				observation.CanonicalTerminalText = ""
				resp.OutputPreview = terminalText
				resp.OutputText = ""
			}
		}
		for _, content := range staged {
			observation.DiagnosticContent = append(observation.DiagnosticContent, content.Reference)
		}
		resp.ProviderInvocationID = firstNonEmpty(strings.TrimSpace(resp.ProviderInvocationID), strings.TrimSpace(invocationID))
		resp.ProviderInvocationAt = firstNonEmpty(strings.TrimSpace(resp.ProviderInvocationAt), strings.TrimSpace(req.InvocationIssuedAt))
		resp.ProviderSessionID = firstNonEmpty(strings.TrimSpace(resp.ProviderSessionID), observation.ProviderSessionID)
		resp.Tokens = observation.Tokens
		resp = withInvocationStreamObservation(resp, observation)
		return withExecuteProviderDiagnostics(resp, buildProviderDiagnosticsJSON(req, diagnostics, execDiag, stderrSnapshot, providerRetryAt, providerRetryAfterMS))
	}
	inputTimeout := a.streamInactivityTimeout
	if inputTimeout <= 0 {
		inputTimeout = a.firstEventTimeout
	}
	if inputErr := writeOpenCodePrompt(attemptCtx, startedRun.handle, nil, inputTimeout, func(inputCtx context.Context, w io.Writer) error {
		return writeRequestPrompt(inputCtx, req, invocationID, w)
	}); inputErr != nil {
		_ = startedRun.handle.Cancel(context.Background())
		_ = stdout.Close()
		execDiag.CmdWaitStartedAt = time.Now().UTC().Format(time.RFC3339)
		guardResult, capturedStderr, waitErr := awaitGuardedRun(req, 1, startedRun.handle, stderrDone)
		processGuards = append(processGuards[:0], processGuardObservation(1, guardResult))
		execDiag.CmdWaitFinishedAt = time.Now().UTC().Format(time.RFC3339)
		execDiag.WaitReturned = true
		allProcessesExited = guardResult.Quiescent
		if waitErr != nil {
			execDiag.WaitError = waitErr.Error()
		}
		exitCode := guardResult.ExitCode
		execDiag.ExitCode = &exitCode
		if guardResult.ExitSignal != 0 {
			exitSignal := guardResult.ExitSignal
			execDiag.ExitSignal = &exitSignal
		}
		diagnostics.ProcessExitedAt = time.Now().UTC().Format(time.RFC3339)
		a.clearInFlight(req.AttemptID)
		execDiag.StderrLineCount = capturedStderr.lineCount
		execDiag.LastStderrLineAt = capturedStderr.lastLineAt
		execDiag.StderrEOFSeen = capturedStderr.eofSeen
		stderrSnapshot = capturedStderr
		allStderrDrained = capturedStderr.eofSeen
		failureText := inputErr.Error()
		var result attemptCompletionResult
		var inputTimeout *providerTimeoutError
		switch {
		case isExecutionCancelled(attemptCtx.Err()) || (isExecutionCancelled(inputErr) && !errors.As(inputErr, &inputTimeout)):
			result = executionResultForCancellation(inputErr, actualSessionID, "OpenCode prompt input was cancelled")
		case isExecutionTimedOut(attemptCtx.Err()) || errors.As(inputErr, &inputTimeout):
			timeoutErr := inputErr
			if isExecutionTimedOut(attemptCtx.Err()) {
				timeoutErr = attemptCtx.Err()
			}
			result = executionResultForTimeout(timeoutErr, actualSessionID)
			inputTimedOut = true
		default:
			result = attemptCompletionResult{
				State:             attemptCompletionStateFailed,
				ReasonCode:        "provider_unknown_failure",
				FailureText:       failureText,
				ProviderSessionID: actualSessionID,
				EvidenceSource:    completionEvidenceSourceStream,
			}
		}
		failureText = firstNonEmpty(strings.TrimSpace(result.FailureText), failureText)
		terminalType := AttemptEventTypeFailed
		payloadMap := map[string]any{
			"error":                      failureText,
			"stream_diagnostics":         diagnostics.toMap(),
			"execution_path_diagnostics": execDiag.toMap(),
		}
		if result.State == attemptCompletionStateCancelled {
			terminalType = AttemptEventTypeCancelled
			inputCancelled = true
			inputTimedOut = false
			payloadMap = map[string]any{"reason": failureText}
		}
		payload, _ := json.Marshal(payloadMap)
		appended, existingTerminalType := a.appendTerminalEventOnceWithType(req.AttemptID, AttemptStreamEvent{
			AttemptID:   req.AttemptID,
			EventType:   terminalType,
			PayloadJSON: string(payload),
			OccurredAt:  time.Now().UTC().Format(time.RFC3339),
		})
		if !appended && existingTerminalType == AttemptEventTypeCancelled {
			result = executionResultForCancellation(context.Canceled, actualSessionID, "OpenCode prompt input was cancelled")
			failureText = result.FailureText
			inputCancelled = true
			inputTimedOut = false
		}
		inputTerminalSet = true
		if terminalType == AttemptEventTypeCancelled || existingTerminalType == AttemptEventTypeCancelled {
			execDiag.CancelledEventAppended = true
		} else {
			execDiag.FailedEventAppended = appended
		}
		return finish(executeResponseForCompletionResult(result)), errors.Join(executeErrorForCompletionResult(result), inputErr, waitErr)
	}
	watchdog = startCommandWatchdog(startedRun.handle, a.firstEventTimeout, a.streamInactivityTimeout)
	if err := decodeOpencodeRunEventStream(io.TeeReader(stdout, capture.stdout), nil, func(event opencodeRunEvent) error {
		startedRun.databaseStartupObserved()
		now := time.Now().UTC()
		diagnostics.EventCount++
		if lastEventAt.IsZero() {
			diagnostics.FirstEventLatency = now.Sub(startAt)
			diagnostics.FirstEventAt = now.Format(time.RFC3339)
		} else {
			gap := now.Sub(lastEventAt)
			if gap > diagnostics.MaxInterEventGap {
				diagnostics.MaxInterEventGap = gap
			}
		}
		lastEventAt = now
		diagnostics.LastEventAt = now.Format(time.RFC3339)

		watchdog.Signal()

		if strings.TrimSpace(event.SessionID) != "" {
			actualSessionID = a.observeProviderSession(req, actualSessionID, event.SessionID, &providerSessionObserved)
			execDiag.SessionID = actualSessionID
		}
		execDiag.LastStdoutEventAt = now.Format(time.RFC3339)
		execDiag.StdoutEventCount++
		if strings.TrimSpace(event.Type) == "error" {
			providerErrorSeen = true
			upstreamErrorText = firstNonEmpty(strings.TrimSpace(event.ErrorText), upstreamErrorText)
			providerErrorCode = firstNonEmpty(strings.TrimSpace(event.ProviderErrorCode), providerErrorCode)
			providerErrorCategory = firstNonEmpty(strings.TrimSpace(event.ProviderErrorCategory), providerErrorCategory)
			if event.ProviderErrorStatus != 0 {
				providerErrorStatus = event.ProviderErrorStatus
			}
			execDiag.UpstreamErrorText = upstreamErrorText
			providerRetryAt = event.RetryAt
			providerRetryAfterMS = event.RetryAfterMS
		}

		acceptText := responseState.observe(event)
		execDiag.StepFinishSeen = responseState.completed.sequence > 0
		execDiag.LastStepFinishReason = strings.TrimSpace(responseState.completed.reason.Normalized)
		execDiag.LastStepFinishReasonKind = string(responseState.completed.reason.Kind)
		execDiag.TerminalStateClass = string(responseState.completed.reason.Class)
		if event.Type == "step_finish" && event.Part.Tokens != nil {
			accumulatedTokens.InputTokens += event.Part.Tokens.Input
			accumulatedTokens.OutputTokens += event.Part.Tokens.Output
			accumulatedTokens.TotalTokens += event.Part.Tokens.Total
			accumulatedTokens.ReasoningTokens += event.Part.Tokens.Reasoning
			accumulatedTokens.CacheReadTokens += event.Part.Tokens.Cache.Read
			accumulatedTokens.CacheWriteTokens += event.Part.Tokens.Cache.Write
		}
		if toolID := firstNonEmpty(strings.TrimSpace(event.Part.CallID), strings.TrimSpace(event.Part.ID)); toolID != "" && isToolRunEvent(event) {
			if err := tools.observe(toolID, strings.TrimSpace(event.Part.Tool), strings.ToLower(strings.TrimSpace(event.Part.State.Status))); err != nil {
				return err
			}
			meaningfulProgress = true
		}
		if normalizeActivityEventType(event.Type) == "reasoning" || normalizeActivityEventType(event.Part.Type) == "reasoning" {
			meaningfulProgress = true
		}

		emitActivityObservation(req, event)

		if event.Type != "text" || !event.hasText() {
			return nil
		}
		execDiag.LastProgressTextLen = len(event.Part.Text)
		sum := sha256.Sum256([]byte(event.Part.Text))
		execDiag.LastProgressTextSHA = fmt.Sprintf("%x", sum[:])
		if event.textContent != nil {
			execDiag.LastProgressTextLen = int(event.textContent.size)
			execDiag.LastProgressTextSHA = event.textContent.sha256
		}

		meaningfulProgress = true
		return event.textChunks(func(text string, first bool) error {
			if first {
				textChunks.append(text)
			} else {
				textChunks.write(text)
			}
			if acceptText {
				responseState.appendChunk(text, first)
			}
			if err := firstNonNilError(textChunks.err, responseState.err); err != nil {
				return err
			}
			if req.OnProgress != nil {
				req.OnProgress(text)
			}
			payload, _ := json.Marshal(map[string]any{
				"text":                                text,
				"execution_path_diagnostics_snapshot": execDiag.progressSnapshot(),
			})
			a.appendEventUnlessTerminal(req.AttemptID, AttemptStreamEvent{
				AttemptID:   req.AttemptID,
				EventType:   AttemptEventTypeProgress,
				PayloadJSON: string(payload),
				OccurredAt:  time.Now().UTC().Format(time.RFC3339),
			})
			return a.eventJournalError(req.AttemptID)
		})
	}, func(digest string, err error) {
		recordMalformedStdoutDigest(&execDiag, digest)
	}, watchdog.Signal); err != nil {
		if execDiag.StdoutMalformedLineCount == 0 {
			recordMalformedStdoutLine(&execDiag, err.Error())
		}
		watchdog.Stop()
		_ = startedRun.handle.Cancel(context.Background())
		execDiag.CmdWaitStartedAt = time.Now().UTC().Format(time.RFC3339)
		guardResult, stderrSnapshot, waitErr := awaitGuardedRun(req, 1, startedRun.handle, stderrDone)
		processGuards = append(processGuards[:0], processGuardObservation(1, guardResult))
		execDiag.CmdWaitFinishedAt = time.Now().UTC().Format(time.RFC3339)
		execDiag.WaitReturned = true
		allProcessesExited = guardResult.Quiescent
		if waitErr != nil {
			execDiag.WaitError = waitErr.Error()
		}
		exitCode := guardResult.ExitCode
		execDiag.ExitCode = &exitCode
		if guardResult.ExitSignal != 0 {
			exitSignal := guardResult.ExitSignal
			execDiag.ExitSignal = &exitSignal
		}
		diagnostics.ProcessExitedAt = time.Now().UTC().Format(time.RFC3339)
		a.clearInFlight(req.AttemptID)
		execDiag.StderrLineCount = stderrSnapshot.lineCount
		execDiag.LastStderrLineAt = stderrSnapshot.lastLineAt
		execDiag.StderrEOFSeen = stderrSnapshot.eofSeen
		allStderrDrained = stderrSnapshot.eofSeen
		timeoutErr := watchdog.Err()
		if timeoutErr != nil {
			timeoutErr = decorateTimeoutError(timeoutErr, diagnostics)
		} else if isExecutionTimedOut(attemptCtx.Err()) {
			timeoutErr = attemptCtx.Err()
		}
		if timeoutErr != nil {
			result := executionResultForTimeout(timeoutErr, actualSessionID)
			result.OutputText = strings.TrimSpace(textChunks.preview)
			result.Tokens = accumulatedTokens
			payload, _ := json.Marshal(buildTimeoutFailurePayload(timeoutErr, diagnostics, execDiag))
			execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
				AttemptID:   req.AttemptID,
				EventType:   AttemptEventTypeFailed,
				PayloadJSON: string(payload),
				OccurredAt:  time.Now().UTC().Format(time.RFC3339),
			})
			return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
		}
		outputText := strings.TrimSpace(textChunks.preview)
		result := attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
			ProviderSessionID: actualSessionID,
			OutputText:        outputText,
			Tokens:            accumulatedTokens,
			MalformedProtocol: true,
			FailureText:       fmt.Sprintf("read opencode stream: %v", err),
		}).Result
		payload, _ := json.Marshal(map[string]any{
			"error":                      result.FailureText,
			"stream_diagnostics":         diagnostics.toMap(),
			"execution_path_diagnostics": execDiag.toMap(),
		})
		execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
			AttemptID:   req.AttemptID,
			EventType:   AttemptEventTypeFailed,
			PayloadJSON: string(payload),
			OccurredAt:  time.Now().UTC().Format(time.RFC3339),
		})
		return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
	}
	execDiag.StdoutEOFSeen = true
	allStdoutDrained = true
	watchdog.Stop()

	execDiag.CmdWaitStartedAt = time.Now().UTC().Format(time.RFC3339)
	guardResult, stderrSnapshot, waitErr := awaitGuardedRun(req, 1, startedRun.handle, stderrDone)
	processGuards = append(processGuards[:0], processGuardObservation(1, guardResult))
	execDiag.CmdWaitFinishedAt = time.Now().UTC().Format(time.RFC3339)
	execDiag.WaitReturned = true
	allProcessesExited = guardResult.Quiescent
	if waitErr != nil {
		execDiag.WaitError = waitErr.Error()
	}
	exitCode := guardResult.ExitCode
	execDiag.ExitCode = &exitCode
	if guardResult.ExitSignal != 0 {
		exitSignal := guardResult.ExitSignal
		execDiag.ExitSignal = &exitSignal
	}
	diagnostics.ProcessExitedAt = time.Now().UTC().Format(time.RFC3339)
	a.clearInFlight(req.AttemptID)
	execDiag.StderrLineCount = stderrSnapshot.lineCount
	execDiag.LastStderrLineAt = stderrSnapshot.lastLineAt
	execDiag.StderrEOFSeen = stderrSnapshot.eofSeen
	allStderrDrained = stderrSnapshot.eofSeen
	if strings.TrimSpace(stderrSnapshot.scannerError) != "" {
		if strings.TrimSpace(execDiag.WaitError) == "" {
			execDiag.WaitError = stderrSnapshot.scannerError
		} else {
			execDiag.WaitError = execDiag.WaitError + "; stderr_scan_error=" + stderrSnapshot.scannerError
		}
	}
	stderrText := strings.TrimSpace(string(stderrSnapshot.raw))

	if actualSessionID != "" {
		a.bindActualSession(req.SessionID, actualSessionID)
	}
	partialCombinedText := strings.TrimSpace(textChunks.preview)
	decision := attemptCompletionDecision{}
	result := attemptCompletionResult{}
	adjudicated := false

	timeoutErr := watchdog.Err()
	if timeoutErr != nil {
		timeoutErr = decorateTimeoutError(timeoutErr, diagnostics)
	} else if isExecutionTimedOut(waitErr) || isExecutionTimedOut(attemptCtx.Err()) {
		timeoutErr = firstNonNilError(waitErr, attemptCtx.Err())
	}
	if timeoutErr != nil {
		timeoutResult := executionResultForTimeout(timeoutErr, actualSessionID)
		timeoutResult.OutputText = partialCombinedText
		timeoutResult.Tokens = accumulatedTokens
		execDiag.FailedEventAppended = true
		payload, _ := json.Marshal(buildTimeoutFailurePayload(timeoutErr, diagnostics, execDiag))
		execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
			AttemptID:   req.AttemptID,
			EventType:   AttemptEventTypeFailed,
			PayloadJSON: string(payload),
			OccurredAt:  time.Now().UTC().Format(time.RFC3339),
		})
		return finish(executeResponseForCompletionResult(timeoutResult)), executeErrorForCompletionResult(timeoutResult)
	}

	if isExecutionCancelled(waitErr) || isExecutionCancelled(attemptCtx.Err()) {
		cancelErr := firstNonNilError(waitErr, attemptCtx.Err())
		result := executionResultForCancellation(cancelErr, actualSessionID, firstNonEmpty(errorString(cancelErr), "opencode run cancelled"))
		result.OutputText = partialCombinedText
		result.Tokens = accumulatedTokens
		reason := firstNonEmpty(errorString(cancelErr), "opencode run cancelled")
		payload, _ := json.Marshal(map[string]any{"reason": reason})
		execDiag.CancelledEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
			AttemptID:   req.AttemptID,
			EventType:   AttemptEventTypeCancelled,
			PayloadJSON: string(payload),
			OccurredAt:  time.Now().UTC().Format(time.RFC3339),
		})
		return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
	}

	if waitErr != nil {
		result := executionResultForProcessError(waitErr, actualSessionID, upstreamErrorText, providerErrorSeen, providerErrorCode, providerErrorCategory, providerErrorStatus, firstNonEmpty(stderrText, waitErr.Error()))
		result.OutputText = partialCombinedText
		result.Tokens = accumulatedTokens
		payload, _ := json.Marshal(map[string]any{
			"error":                      result.FailureText,
			"stream_diagnostics":         diagnostics.toMap(),
			"execution_path_diagnostics": execDiag.toMap(),
		})
		execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
			AttemptID:   req.AttemptID,
			EventType:   AttemptEventTypeFailed,
			PayloadJSON: string(payload),
			OccurredAt:  time.Now().UTC().Format(time.RFC3339),
		})
		return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
	}

	if fatalResult, fatal := adjudicateFatalExecutionEvidence(attemptCompletionEvidence{
		ProviderSessionID:           actualSessionID,
		OutputText:                  strings.TrimSpace(responseState.terminalOutput()),
		Tokens:                      accumulatedTokens,
		StepFinishSeen:              execDiag.StepFinishSeen,
		LastStepFinish:              responseState.completed.reason,
		UpstreamErrorText:           upstreamErrorText,
		ProviderErrorPresent:        providerErrorSeen,
		ProviderErrorCode:           providerErrorCode,
		ProviderErrorCategory:       providerErrorCategory,
		ProviderErrorStatus:         providerErrorStatus,
		ConflictingTerminalEvidence: responseState.conflictingTerminalEvidence,
		MalformedProtocol:           execDiag.StdoutMalformedLineCount > 0,
		ProcessFailed:               providerProcessObservationFailed(allProcessesExited, execDiag.ExitCode, execDiag.ExitSignal, allStdoutDrained, allStderrDrained),
		PendingToolCount:            tools.pending(),
		LaterEventAfterTerminal:     responseState.laterEventAfterTerminal(),
	}); fatal {
		result = fatalResult
		adjudicated = true
	}

	if shouldContinueLiveStream(responseState.completed.reason) && !adjudicated {
		for shouldContinueLiveStream(responseState.completed.reason) {
			if attemptCtx.Err() != nil {
				if isExecutionTimedOut(attemptCtx.Err()) {
					result := executionResultForTimeout(attemptCtx.Err(), actualSessionID)
					result.OutputText = strings.TrimSpace(textChunks.preview)
					result.Tokens = accumulatedTokens
					payload, _ := json.Marshal(buildTimeoutFailurePayload(attemptCtx.Err(), diagnostics, execDiag))
					execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
						AttemptID:   req.AttemptID,
						EventType:   AttemptEventTypeFailed,
						PayloadJSON: string(payload),
						OccurredAt:  time.Now().UTC().Format(time.RFC3339),
					})
					return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
				}
				result := executionResultForCancellation(attemptCtx.Err(), actualSessionID, "opencode run cancelled during continuation")
				result.OutputText = strings.TrimSpace(textChunks.preview)
				result.Tokens = accumulatedTokens
				execDiag.CancelledEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
					AttemptID:   req.AttemptID,
					EventType:   AttemptEventTypeCancelled,
					PayloadJSON: `{"reason":"opencode run cancelled during continuation"}`,
					OccurredAt:  time.Now().UTC().Format(time.RFC3339),
				})
				return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
			}
			execDiag.ContinuationTurnsElapsed++
			continuationPayload, _ := json.Marshal(map[string]any{
				"reason":                              "continuation_turn_started",
				"continuation_turn":                   execDiag.ContinuationTurnsElapsed,
				"prior_finish_reason":                 strings.TrimSpace(execDiag.LastStepFinishReason),
				"prior_terminal_class":                strings.TrimSpace(execDiag.TerminalStateClass),
				"execution_path_diagnostics_snapshot": execDiag.progressSnapshot(),
			})
			a.appendEventUnlessTerminal(req.AttemptID, AttemptStreamEvent{
				AttemptID:   req.AttemptID,
				EventType:   AttemptEventTypeProgress,
				PayloadJSON: string(continuationPayload),
				OccurredAt:  time.Now().UTC().Format(time.RFC3339),
			})
			responseState.resetStep()
			continuationResult, continuationErr := a.executeContinuationTurn(attemptCtx, req, actualSessionID, invocationID, execDiag.ContinuationTurnsElapsed+1, capture, tools, &responseState)
			// Every command's ownership and quiescence already crossed the durable
			// callbacks. Retain only the last diagnostic observation in memory;
			// settlement checks every guard row for this invocation and lease.
			processGuards = append(processGuards[:0], processGuardObservation(execDiag.ContinuationTurnsElapsed+1, continuationResult.GuardResult))
			diagnostics.EventCount += continuationResult.EventCount
			execDiag.StdoutMalformedLineCount += continuationResult.MalformedCount
			meaningfulProgress = meaningfulProgress || continuationResult.MeaningfulProgress
			allProcessesExited = allProcessesExited && continuationResult.ProcessExited
			allStdoutDrained = allStdoutDrained && continuationResult.StdoutDrained
			allStderrDrained = allStderrDrained && continuationResult.StderrDrained
			if continuationResult.ExitCode != nil {
				execDiag.ExitCode = continuationResult.ExitCode
			}
			if continuationResult.ExitSignal != nil {
				execDiag.ExitSignal = continuationResult.ExitSignal
			}
			execDiag.LastStepFinishReason = strings.TrimSpace(responseState.completed.reason.Normalized)
			execDiag.LastStepFinishReasonKind = string(responseState.completed.reason.Kind)
			execDiag.TerminalStateClass = string(responseState.completed.reason.Class)
			execDiag.StepFinishSeen = responseState.completed.sequence > 0
			stderrSnapshot = mergeStderrCapture(stderrSnapshot, continuationResult.Stderr)
			execDiag.StderrLineCount = stderrSnapshot.lineCount
			execDiag.LastStderrLineAt = stderrSnapshot.lastLineAt
			execDiag.StderrEOFSeen = stderrSnapshot.eofSeen
			if strings.TrimSpace(continuationResult.WaitError) != "" {
				execDiag.WaitError = strings.TrimSpace(continuationResult.WaitError)
			}
			if strings.TrimSpace(continuationResult.UpstreamErrorText) != "" {
				upstreamErrorText = strings.TrimSpace(continuationResult.UpstreamErrorText)
				execDiag.UpstreamErrorText = upstreamErrorText
				providerRetryAt = continuationResult.RetryAt
				providerRetryAfterMS = continuationResult.RetryAfterMS
			}
			providerErrorSeen = providerErrorSeen || continuationResult.ProviderErrorPresent
			providerErrorCode = firstNonEmpty(continuationResult.ProviderErrorCode, providerErrorCode)
			providerErrorCategory = firstNonEmpty(continuationResult.ProviderErrorCategory, providerErrorCategory)
			if continuationResult.ProviderErrorStatus != 0 {
				providerErrorStatus = continuationResult.ProviderErrorStatus
			}
			continuationTimedOut = continuationTimedOut || continuationResult.TimedOut
			if continuationResult.ProviderSessionID != "" {
				actualSessionID = continuationResult.ProviderSessionID
				a.bindActualSession(req.SessionID, actualSessionID)
				execDiag.SessionID = actualSessionID
			}
			textChunks.append(continuationResult.TextPreview)
			accumulateTokenUsage(&accumulatedTokens, continuationResult.Tokens)
			combinedContinuationText := strings.TrimSpace(responseState.terminalOutput())
			if isExecutionCancelled(continuationErr) {
				result := executionResultForCancellation(continuationErr, actualSessionID, "opencode continuation cancelled")
				result.OutputText = firstNonEmpty(combinedContinuationText, textChunks.preview)
				result.Tokens = accumulatedTokens
				execDiag.CancelledEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
					AttemptID:   req.AttemptID,
					EventType:   AttemptEventTypeCancelled,
					PayloadJSON: `{"reason":"opencode continuation cancelled"}`,
					OccurredAt:  time.Now().UTC().Format(time.RFC3339),
				})
				return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
			}
			if isExecutionTimedOut(continuationErr) {
				timeoutResult := executionResultForTimeout(continuationErr, actualSessionID)
				timeoutResult.OutputText = firstNonEmpty(combinedContinuationText, textChunks.preview)
				timeoutResult.Tokens = accumulatedTokens
				payload, _ := json.Marshal(buildTimeoutFailurePayload(continuationErr, diagnostics, execDiag))
				execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
					AttemptID:   req.AttemptID,
					EventType:   AttemptEventTypeFailed,
					PayloadJSON: string(payload),
					OccurredAt:  time.Now().UTC().Format(time.RFC3339),
				})
				return finish(executeResponseForCompletionResult(timeoutResult)), executeErrorForCompletionResult(timeoutResult)
			}
			if continuationErr != nil {
				failureEvidence := attemptCompletionEvidence{
					ProviderSessionID:     actualSessionID,
					OutputText:            combinedContinuationText,
					Tokens:                accumulatedTokens,
					UpstreamErrorText:     strings.TrimSpace(continuationResult.UpstreamErrorText),
					ProviderErrorPresent:  continuationResult.ProviderErrorPresent,
					ProviderErrorCode:     continuationResult.ProviderErrorCode,
					ProviderErrorCategory: continuationResult.ProviderErrorCategory,
					ProviderErrorStatus:   continuationResult.ProviderErrorStatus,
					FailureText:           continuationErr.Error(),
					MalformedProtocol:     continuationResult.MalformedCount > 0,
				}
				var timeoutTyped *providerTimeoutError
				if errors.As(continuationErr, &timeoutTyped) {
					failureEvidence.Timeout = true
				} else if strings.TrimSpace(failureEvidence.UpstreamErrorText) == "" && !failureEvidence.MalformedProtocol {
					failureEvidence.ProcessFailed = true
				}
				failureResult := attemptCompletionAdjudicator{}.Adjudicate(failureEvidence).Result
				payloadMap := map[string]any{
					"error":                      firstNonEmpty(strings.TrimSpace(failureResult.FailureText), continuationErr.Error()),
					"stream_diagnostics":         diagnostics.toMap(),
					"execution_path_diagnostics": execDiag.toMap(),
				}
				if errors.As(continuationErr, &timeoutTyped) {
					payloadMap["timeout_type"] = strings.TrimSpace(timeoutTyped.TimeoutType)
					payloadMap["timeout_ms"] = timeoutTyped.Timeout.Milliseconds()
				}
				payload, _ := json.Marshal(payloadMap)
				execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
					AttemptID:   req.AttemptID,
					EventType:   AttemptEventTypeFailed,
					PayloadJSON: string(payload),
					OccurredAt:  time.Now().UTC().Format(time.RFC3339),
				})
				return finish(executeResponseForCompletionResult(failureResult)), executeErrorForCompletionResult(failureResult)
			}
			if fatalResult, fatal := adjudicateFatalExecutionEvidence(attemptCompletionEvidence{
				ProviderSessionID:           actualSessionID,
				OutputText:                  combinedContinuationText,
				Tokens:                      accumulatedTokens,
				StepFinishSeen:              execDiag.StepFinishSeen,
				LastStepFinish:              responseState.completed.reason,
				UpstreamErrorText:           upstreamErrorText,
				ProviderErrorPresent:        providerErrorSeen,
				ProviderErrorCode:           providerErrorCode,
				ProviderErrorCategory:       providerErrorCategory,
				ProviderErrorStatus:         providerErrorStatus,
				ConflictingTerminalEvidence: responseState.conflictingTerminalEvidence,
				MalformedProtocol:           execDiag.StdoutMalformedLineCount > 0,
				ProcessFailed:               providerProcessObservationFailed(allProcessesExited, execDiag.ExitCode, execDiag.ExitSignal, allStdoutDrained, allStderrDrained),
				PendingToolCount:            tools.pending(),
				LaterEventAfterTerminal:     responseState.laterEventAfterTerminal(),
			}); fatal {
				result = fatalResult
				adjudicated = true
				break
			}
		}
	}

	observedCombinedText := strings.TrimSpace(textChunks.preview)
	canonicalCombinedText := strings.TrimSpace(responseState.terminalOutput())
	if !adjudicated {
		decision = attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
			ProviderSessionID:           actualSessionID,
			OutputText:                  canonicalCombinedText,
			Tokens:                      accumulatedTokens,
			StepFinishSeen:              execDiag.StepFinishSeen,
			LastStepFinish:              responseState.completed.reason,
			UpstreamErrorText:           upstreamErrorText,
			ProviderErrorPresent:        providerErrorSeen,
			ProviderErrorCode:           providerErrorCode,
			ProviderErrorCategory:       providerErrorCategory,
			ProviderErrorStatus:         providerErrorStatus,
			ConflictingTerminalEvidence: responseState.conflictingTerminalEvidence,
			MalformedProtocol:           execDiag.StdoutMalformedLineCount > 0,
			CleanEOF:                    waitErr == nil,
			PendingToolCount:            tools.pending(),
			LaterEventAfterTerminal:     responseState.laterEventAfterTerminal(),
		})
		result = decision.Result
	}

	if result.State != attemptCompletionStateSuccess {
		if canonicalCombinedText == "" && responseState.completed.reason.Class != stepFinishTerminalSuccessCandidate {
			result.OutputText = observedCombinedText
		}
		reason := firstNonEmpty(result.ReasonCode, execDiag.LastStepFinishReasonKind, execDiag.LastStepFinishReason)
		if result.State == attemptCompletionStateIncompleteTurn {
			reason = firstNonEmpty(execDiag.LastStepFinishReason, execDiag.LastStepFinishReasonKind, result.ReasonCode)
		}
		errorText := firstNonEmpty(result.FailureText, stderrText, fmt.Sprintf("incomplete terminal state: %s", reason))
		payload, _ := json.Marshal(map[string]any{
			"error":                      errorText,
			"stream_diagnostics":         diagnostics.toMap(),
			"execution_path_diagnostics": execDiag.toMap(),
		})
		eventType := AttemptEventTypeFailed
		var returnErr error
		if result.State == attemptCompletionStateCancelled {
			eventType = AttemptEventTypeCancelled
			returnErr = context.Canceled
		} else if result.State == attemptCompletionStateIncompleteTurn {
			returnErr = fmt.Errorf("opencode run failed: %w", &IncompleteTerminalStateError{Reason: reason})
		} else {
			returnErr = fmt.Errorf("opencode run failed: %s", errorText)
		}
		if eventType == AttemptEventTypeCancelled {
			execDiag.CancelledEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{AttemptID: req.AttemptID, EventType: eventType, PayloadJSON: string(payload), OccurredAt: time.Now().UTC().Format(time.RFC3339)})
			return finish(executeResponseForCompletionResult(result)), returnErr
		}
		execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{AttemptID: req.AttemptID, EventType: eventType, PayloadJSON: string(payload), OccurredAt: time.Now().UTC().Format(time.RFC3339)})
		return finish(executeResponseForCompletionResult(result)), returnErr
	}

	combinedText := strings.TrimSpace(result.OutputText)
	execDiag.CompletionGatePassed = true
	payload, _ := json.Marshal(map[string]any{
		"session_id":                 actualSessionID,
		"output":                     combinedText,
		"stream_diagnostics":         diagnostics.toMap(),
		"execution_path_diagnostics": execDiag.toMap(),
	})
	execDiag.SucceededEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
		AttemptID:   req.AttemptID,
		EventType:   AttemptEventTypeSucceeded,
		PayloadJSON: string(payload),
		OccurredAt:  time.Now().UTC().Format(time.RFC3339),
	})

	return finish(executeResponseForCompletionResult(result)), nil
}

func buildProviderDiagnosticsJSON(req ExecuteRequest, diagnostics streamDiagnostics, execDiagnostics executionPathDiagnostics, stderr stderrCapture, retryAt string, retryAfterMS *float64) string {
	payload := map[string]any{
		"stream_diagnostics":         diagnostics.toMap(),
		"execution_path_diagnostics": execDiagnostics.toMap(),
	}
	if stderr.lineCount > 0 {
		payload["stderr_line_count"] = stderr.lineCount
	}
	if strings.TrimSpace(stderr.lastLineAt) != "" {
		payload["stderr_last_line_at"] = strings.TrimSpace(stderr.lastLineAt)
	}
	if stderr.eofSeen {
		payload["stderr_eof_seen"] = true
	}
	if tail := tailString(string(stderr.raw), 4096); tail != "" {
		payload["stderr_tail"] = tail
	}
	if strings.TrimSpace(stderr.scannerError) != "" {
		payload["stderr_scan_error"] = strings.TrimSpace(stderr.scannerError)
	}
	if strings.TrimSpace(retryAt) != "" {
		payload["retry_at"] = strings.TrimSpace(retryAt)
	}
	if retryAfterMS != nil {
		payload["retry_after_ms"] = *retryAfterMS
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

func tailString(raw string, maxBytes int) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || maxBytes <= 0 || len(trimmed) <= maxBytes {
		return trimmed
	}
	return trimmed[len(trimmed)-maxBytes:]
}

func emitActivityObservation(req ExecuteRequest, event opencodeRunEvent) {
	if req.OnActivity == nil {
		return
	}
	observation := ExecuteActivityObservation{
		ProviderPartID: strings.TrimSpace(event.Part.ID),
		ToolCallID:     strings.TrimSpace(event.Part.CallID),
		InvocationID:   strings.TrimSpace(req.InvocationID),
		Input:          event.activityContent["input"].Normalized(),
		Output:         event.activityContent["output"].Normalized(),
		Reasoning:      providerexecution.ActivityContent{Availability: "not_provided"},
		Message:        providerexecution.ActivityContent{Availability: "not_provided"},
		OccurredAt:     activityObservationOccurredAt(event),
	}
	switch normalizeActivityEventType(event.Type) {
	case "tool", "tool_call", "tool_result", "tool_use":
		observation.Kind = ExecuteActivityKindTool
		observation.ToolName = strings.TrimSpace(event.Part.Tool)
		observation.ToolStatus = firstNonEmpty(strings.TrimSpace(event.Part.State.Status), strings.TrimSpace(event.Part.Reason))
		observation.ToolTitle = strings.TrimSpace(event.Part.State.Title)
	case "text":
		observation.Kind = ExecuteActivityKindMessage
		observation.Message = event.activityContent["text"].Normalized()
		if observation.Message.Availability != "available" {
			return
		}
	case "reasoning":
		observation.Reasoning = event.activityContent["text"].Normalized()
		observation.Kind = ExecuteActivityKindReasoning
		if observation.Reasoning.Availability != "available" {
			observation.Summary = reasoningDetailFromMetadata(
				strings.TrimSpace(event.Part.Metadata.Redacted),
				strings.TrimSpace(event.Part.Metadata.OpenAI.ReasoningEncryptedContent),
			)
			if observation.Summary != "" {
				observation.Reasoning = providerexecution.ActivityContent{Availability: "unavailable"}
			}
		}
	case "step_start":
		observation.Kind = ExecuteActivityKindProgress
		observation.Label = "Provider step started"
	case "step_finish":
		if !stepFinishHasTokenUsage(event) {
			return
		}
		observation.Kind = ExecuteActivityKindProgress
		observation.Label = "Provider Step Completed"
	default:
		return
	}
	req.OnActivity(observation)
}

func stepFinishHasTokenUsage(event opencodeRunEvent) bool {
	tokens := event.Part.Tokens
	return tokens != nil && (tokens.Total > 0 ||
		tokens.Input > 0 ||
		tokens.Output > 0 ||
		tokens.Reasoning > 0 ||
		tokens.Cache.Read > 0 ||
		tokens.Cache.Write > 0)
}

// activityObservationOccurredAt prefers the part's own end/start timestamp
// (epoch millis) so live activity shares the same occurred-at as the part the
// diagnostic export parser reads, falling back to wall-clock when the
// provider omits a part time.
func activityObservationOccurredAt(event opencodeRunEvent) string {
	for _, ms := range []*int64{event.Part.Time.End, event.Part.Time.Start} {
		if ms != nil && *ms > 0 {
			return time.UnixMilli(*ms).UTC().Format(time.RFC3339)
		}
	}
	return time.Now().UTC().Format(time.RFC3339)
}

func normalizeActivityEventType(value string) string {
	trimmed := strings.ToLower(strings.TrimSpace(value))
	if trimmed == "" {
		return ""
	}
	return strings.ReplaceAll(trimmed, "-", "_")
}

func isToolRunEvent(event opencodeRunEvent) bool {
	switch normalizeActivityEventType(event.Type) {
	case "tool", "tool_call", "tool_result", "tool_use":
		return true
	}
	switch normalizeActivityEventType(event.Part.Type) {
	case "tool", "tool_call", "tool_result", "tool_use":
		return true
	default:
		return false
	}
}

func (a *SessionAdapter) Cancel(ctx context.Context, req CancelRequest) error {
	if strings.TrimSpace(req.AttemptID) == "" {
		return fmt.Errorf("attempt id is required")
	}
	if strings.TrimSpace(req.OccurredAt) == "" {
		return fmt.Errorf("occurred_at is required")
	}

	a.mu.Lock()
	_, knownAttempt := a.attempts[req.AttemptID]
	handle := a.inFlight[req.AttemptID]
	cancel := a.attemptCancels[req.AttemptID]
	done := a.attemptDone[req.AttemptID]
	a.mu.Unlock()

	if !knownAttempt {
		return &AttemptNotFoundError{AttemptID: req.AttemptID}
	}

	if cancel != nil {
		cancel()
	}
	if handle != nil {
		if err := handle.Cancel(context.Background()); err != nil {
			return fmt.Errorf("cancel guarded opencode process: %w", err)
		}
	}

	payload, _ := json.Marshal(map[string]any{"reason": req.Reason})
	a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
		AttemptID:   req.AttemptID,
		EventType:   AttemptEventTypeCancelled,
		PayloadJSON: string(payload),
		OccurredAt:  req.OccurredAt,
	})
	if done != nil {
		if ctx == nil {
			ctx = context.Background()
		}
		select {
		case <-done:
		case <-ctx.Done():
			return fmt.Errorf("wait for cancelled opencode attempt %q to stop: %w", req.AttemptID, ctx.Err())
		}
	}

	return nil
}

func (a *SessionAdapter) executeContinuationTurn(ctx context.Context, req ExecuteRequest, sessionID, invocationID string, commandOrdinal int, capture *invocationCapture, tools *toolStateStorage, responseState *responseStream) (continuationTurnResult, error) {
	startedRun, err := a.startOpencodeRun(ctx, req, sessionID, commandOrdinal)
	if err != nil {
		return continuationTurnResult{}, err
	}
	defer startedRun.databaseStartupObserved()
	stdout := startedRun.stdout
	stderr := startedRun.stderr
	defer stdout.Close()
	defer stderr.Close()
	a.setInFlight(req.AttemptID, startedRun.handle)
	defer a.clearInFlight(req.AttemptID)

	ctxCancelDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = startedRun.handle.Cancel(context.Background())
		case <-ctxCancelDone:
		}
	}()
	defer close(ctxCancelDone)

	stderrDone := startStderrCapture(io.TeeReader(stderr, capture.stderr))

	result := continuationTurnResult{
		ProviderSessionID: strings.TrimSpace(sessionID),
	}
	providerSessionObserved := false
	continuationPrompt := providerInvocationPromptParts("Continue from the pending tool-call state and finish the response.", req.AttemptID, invocationID)
	inputTimeout := a.streamInactivityTimeout
	if inputTimeout <= 0 {
		inputTimeout = a.firstEventTimeout
	}
	if inputErr := writeOpenCodePrompt(ctx, startedRun.handle, continuationPrompt, inputTimeout); inputErr != nil {
		_ = startedRun.handle.Cancel(context.Background())
		_ = stdout.Close()
		guardResult, capturedStderr, waitErr := awaitGuardedRun(req, commandOrdinal, startedRun.handle, stderrDone)
		result.GuardResult = guardResult
		result.ProcessExited = guardResult.Quiescent
		exitCode := guardResult.ExitCode
		result.ExitCode = &exitCode
		if guardResult.ExitSignal != 0 {
			exitSignal := guardResult.ExitSignal
			result.ExitSignal = &exitSignal
		}
		result.StdoutDrained = false
		result.Stderr = capturedStderr
		result.StderrDrained = capturedStderr.eofSeen
		var inputTimeout *providerTimeoutError
		result.TimedOut = isExecutionTimedOut(ctx.Err()) || errors.As(inputErr, &inputTimeout)
		if waitErr != nil {
			result.WaitError = waitErr.Error()
		}
		return result, errors.Join(inputErr, waitErr)
	}
	watchdog := startCommandWatchdog(startedRun.handle, a.firstEventTimeout, a.streamInactivityTimeout)
	defer watchdog.Stop()

	streamErr := decodeOpencodeRunEventStream(io.TeeReader(stdout, capture.stdout), nil, func(event opencodeRunEvent) error {
		startedRun.databaseStartupObserved()
		watchdog.Signal()
		result.EventCount++
		if strings.TrimSpace(event.SessionID) != "" {
			result.ProviderSessionID = a.observeProviderSession(req, result.ProviderSessionID, event.SessionID, &providerSessionObserved)
		}
		if strings.TrimSpace(event.Type) == "error" {
			result.ProviderErrorPresent = true
			result.UpstreamErrorText = strings.TrimSpace(event.ErrorText)
			result.ProviderErrorCode = strings.TrimSpace(event.ProviderErrorCode)
			result.ProviderErrorCategory = strings.TrimSpace(event.ProviderErrorCategory)
			result.ProviderErrorStatus = event.ProviderErrorStatus
			result.RetryAt = event.RetryAt
			result.RetryAfterMS = event.RetryAfterMS
		}
		acceptText := responseState.observe(event)
		if event.Type == "step_finish" {
			if event.Part.Tokens != nil {
				result.Tokens.InputTokens += event.Part.Tokens.Input
				result.Tokens.OutputTokens += event.Part.Tokens.Output
				result.Tokens.TotalTokens += event.Part.Tokens.Total
				result.Tokens.ReasoningTokens += event.Part.Tokens.Reasoning
				result.Tokens.CacheReadTokens += event.Part.Tokens.Cache.Read
				result.Tokens.CacheWriteTokens += event.Part.Tokens.Cache.Write
			}
		}
		if toolID := firstNonEmpty(strings.TrimSpace(event.Part.CallID), strings.TrimSpace(event.Part.ID)); toolID != "" && isToolRunEvent(event) {
			if err := tools.observe(toolID, strings.TrimSpace(event.Part.Tool), strings.ToLower(strings.TrimSpace(event.Part.State.Status))); err != nil {
				return err
			}
			result.MeaningfulProgress = true
		}
		if normalizeActivityEventType(event.Type) == "reasoning" || normalizeActivityEventType(event.Part.Type) == "reasoning" {
			result.MeaningfulProgress = true
		}
		emitActivityObservation(req, event)
		if event.Type == "text" && event.hasText() {
			return event.textChunks(func(text string, first bool) error {
				if len(result.TextPreview) < streamPreviewBytes {
					result.TextPreview += text[:min(len(text), streamPreviewBytes-len(result.TextPreview))]
				}
				result.MeaningfulProgress = true
				if acceptText {
					responseState.appendChunk(text, first)
				}
				if responseState.err != nil {
					return responseState.err
				}
				if req.OnProgress != nil {
					req.OnProgress(text)
				}
				sum := sha256.Sum256([]byte(text))
				payload, _ := json.Marshal(map[string]any{
					"text": text,
					"execution_path_diagnostics_snapshot": map[string]any{
						"last_progress_text_len": len(text),
						"last_progress_text_sha": fmt.Sprintf("%x", sum[:]),
					},
				})
				a.appendEventUnlessTerminal(req.AttemptID, AttemptStreamEvent{
					AttemptID:   req.AttemptID,
					EventType:   AttemptEventTypeProgress,
					PayloadJSON: string(payload),
					OccurredAt:  time.Now().UTC().Format(time.RFC3339),
				})
				return a.eventJournalError(req.AttemptID)
			})
		}
		return a.eventJournalError(req.AttemptID)
	}, func(_ string, _ error) {
		result.MalformedCount++
	}, watchdog.Signal)
	result.StdoutDrained = streamErr == nil
	if streamErr != nil {
		_ = startedRun.handle.Cancel(context.Background())
	} else {
		watchdog.Stop()
	}
	guardResult, stderrSnapshot, waitErr := awaitGuardedRun(req, commandOrdinal, startedRun.handle, stderrDone)
	result.GuardResult = guardResult
	result.ProcessExited = guardResult.Quiescent
	result.Stderr = stderrSnapshot
	result.StderrDrained = result.Stderr.eofSeen
	streamErr = firstNonNilError(streamErr, responseState.err, responseState.terminal.err)
	exitCode := guardResult.ExitCode
	result.ExitCode = &exitCode
	if guardResult.ExitSignal != 0 {
		exitSignal := guardResult.ExitSignal
		result.ExitSignal = &exitSignal
	}
	if waitErr != nil {
		result.WaitError = waitErr.Error()
	}
	if strings.TrimSpace(result.Stderr.scannerError) != "" {
		if strings.TrimSpace(result.WaitError) == "" {
			result.WaitError = result.Stderr.scannerError
		} else {
			result.WaitError += "; stderr_scan_error=" + result.Stderr.scannerError
		}
	}
	if timeoutErr := watchdog.Err(); timeoutErr != nil {
		result.TimedOut = true
		return result, decorateTimeoutError(timeoutErr, streamDiagnostics{})
	}
	if isExecutionCancelled(waitErr) || isExecutionCancelled(ctx.Err()) {
		return result, context.Canceled
	}
	if isExecutionTimedOut(waitErr) || isExecutionTimedOut(ctx.Err()) {
		return result, context.DeadlineExceeded
	}
	if streamErr != nil {
		return result, fmt.Errorf("read opencode continuation stream: %w", streamErr)
	}
	if result.MalformedCount > 0 {
		return result, fmt.Errorf("opencode continuation emitted %d malformed stream event(s)", result.MalformedCount)
	}
	if waitErr != nil {
		failureText := firstNonEmpty(strings.TrimSpace(result.UpstreamErrorText), strings.TrimSpace(string(result.Stderr.raw)), waitErr.Error())
		return result, fmt.Errorf("opencode continuation failed: %s", failureText)
	}
	if strings.TrimSpace(result.UpstreamErrorText) != "" {
		return result, fmt.Errorf("opencode continuation failed: %s", result.UpstreamErrorText)
	}

	return result, nil
}

func (a *SessionAdapter) Stream(ctx context.Context, req StreamRequest) (<-chan AttemptStreamEvent, <-chan error, error) {
	if strings.TrimSpace(req.AttemptID) == "" {
		return nil, nil, fmt.Errorf("attempt id is required")
	}

	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, nil, fmt.Errorf("provider adapter is closed")
	}
	record, ok := a.attempts[req.AttemptID]
	if !ok || record.releaseRequested {
		a.mu.Unlock()
		return nil, nil, &AttemptNotFoundError{AttemptID: req.AttemptID}
	}
	reader, err := record.reader()
	if err == nil {
		a.streamWG.Add(1)
		record.readers++
	}
	a.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}

	eventCh := make(chan AttemptStreamEvent)
	errCh := make(chan error, 1)

	go func() {
		defer a.streamWG.Done()
		defer a.finishReplayReader(req.AttemptID)
		defer close(eventCh)
		defer close(errCh)
		defer reader.Close()
		decoder := json.NewDecoder(reader)
		for {
			select {
			case <-a.closeCh:
				return
			default:
			}
			var event AttemptStreamEvent
			if err := decoder.Decode(&event); err != nil {
				if err != io.EOF {
					errCh <- err
				}
				return
			}
			select {
			case <-a.closeCh:
				return
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case eventCh <- event:
			}
		}
	}()

	return eventCh, errCh, nil
}

func (a *SessionAdapter) InFlightSnapshot(attemptID string) map[string]any {
	trimmedAttemptID := strings.TrimSpace(attemptID)
	if trimmedAttemptID == "" {
		return map[string]any{"attempt_id": "", "attempt_known": false, "in_flight_present": false}
	}

	a.mu.Lock()
	record, attemptKnown := a.attempts[trimmedAttemptID]
	handle := a.inFlight[trimmedAttemptID]
	streamEventCount := 0
	if attemptKnown {
		streamEventCount = record.count
	}
	a.mu.Unlock()

	snapshot := map[string]any{
		"attempt_id":        trimmedAttemptID,
		"attempt_known":     attemptKnown,
		"in_flight_present": handle != nil,
	}
	if attemptKnown {
		snapshot["stream_event_count"] = streamEventCount
	}
	return snapshot
}

func (a *SessionAdapter) setInFlight(attemptID string, handle processguardian.Handle) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.inFlight[attemptID] = handle
}

func (a *SessionAdapter) clearInFlight(attemptID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.inFlight, attemptID)
}

func (a *SessionAdapter) setAttemptCancel(attemptID string, cancel context.CancelFunc) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.attemptCancels[attemptID] = cancel
}

func (a *SessionAdapter) clearAttemptCancel(attemptID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.attemptCancels, attemptID)
}

func (a *SessionAdapter) beginAttemptOperation(attemptID string) (chan struct{}, *sessionAttemptRecord, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, nil, fmt.Errorf("provider adapter is closed")
	}
	if a.attemptDone[attemptID] != nil {
		return nil, nil, fmt.Errorf("provider attempt is already running")
	}
	if a.attempts[attemptID] != nil {
		return nil, nil, fmt.Errorf("previous provider attempt storage has not been released")
	}
	record := &sessionAttemptRecord{}
	a.attempts[attemptID] = record
	done := make(chan struct{})
	a.attemptDone[attemptID] = done
	return done, record, nil
}

func (a *SessionAdapter) endAttemptOperation(attemptID string, done chan struct{}) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.attemptDone[attemptID] != done {
		return
	}
	delete(a.attemptDone, attemptID)
	close(done)
}

func (a *SessionAdapter) appendEvent(attemptID string, event AttemptStreamEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed && a.attemptDone[attemptID] == nil {
		return
	}
	record, ok := a.attempts[attemptID]
	if !ok {
		record = &sessionAttemptRecord{}
		a.attempts[attemptID] = record
	}
	if strings.TrimSpace(event.OccurredAt) == "" {
		event.OccurredAt = time.Now().UTC().Format(time.RFC3339)
	}
	record.append(event)
}

func (a *SessionAdapter) appendTerminalEventOnce(attemptID string, event AttemptStreamEvent) bool {
	appended, _ := a.appendTerminalEventOnceWithType(attemptID, event)
	return appended
}

func (a *SessionAdapter) appendTerminalEventOnceWithType(attemptID string, event AttemptStreamEvent) (bool, string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed && a.attemptDone[attemptID] == nil {
		return false, ""
	}
	record, ok := a.attempts[attemptID]
	if !ok {
		record = &sessionAttemptRecord{}
		a.attempts[attemptID] = record
	}
	if record.terminal != "" {
		return false, record.terminal
	}
	if strings.TrimSpace(event.OccurredAt) == "" {
		event.OccurredAt = time.Now().UTC().Format(time.RFC3339)
	}
	record.append(event)
	return record.err == nil, record.terminal
}

func (a *SessionAdapter) appendEventUnlessTerminal(attemptID string, event AttemptStreamEvent) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed && a.attemptDone[attemptID] == nil {
		return false
	}
	record := a.attempts[attemptID]
	if record == nil {
		record = &sessionAttemptRecord{}
		a.attempts[attemptID] = record
	}
	if record.terminal != "" {
		return false
	}
	if strings.TrimSpace(event.OccurredAt) == "" {
		event.OccurredAt = time.Now().UTC().Format(time.RFC3339)
	}
	record.append(event)
	return record.err == nil
}

func isTerminalAttemptEvent(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case AttemptEventTypeSucceeded, AttemptEventTypeFailed, AttemptEventTypeCancelled:
		return true
	default:
		return false
	}
}

func (a *SessionAdapter) actualSessionID(logicalSessionID string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return strings.TrimSpace(a.logicalToActual[logicalSessionID])
}

func (a *SessionAdapter) bindActualSession(logicalSessionID, actualSessionID string) {
	logicalSessionID = strings.TrimSpace(logicalSessionID)
	actualSessionID = strings.TrimSpace(actualSessionID)
	if logicalSessionID == "" || actualSessionID == "" {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.logicalToActual[logicalSessionID] = actualSessionID
}

func (a *SessionAdapter) observeProviderSession(req ExecuteRequest, currentSessionID, observedSessionID string, reported *bool) string {
	observedSessionID = strings.TrimSpace(observedSessionID)
	if observedSessionID == "" {
		return strings.TrimSpace(currentSessionID)
	}
	a.bindActualSession(req.SessionID, observedSessionID)
	shouldReport := strings.TrimSpace(currentSessionID) != observedSessionID
	if !shouldReport && req.OnProviderSession != nil && strings.TrimSpace(req.ProviderSessionID) == observedSessionID {
		shouldReport = reported == nil || !*reported
	}
	if shouldReport && req.OnProviderSession != nil {
		req.OnProviderSession(observedSessionID)
	}
	if reported != nil {
		*reported = true
	}
	return observedSessionID
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

type commandWatchdog struct {
	signal chan struct{}
	stop   chan struct{}
	done   chan struct{}

	mu           sync.Mutex
	err          error
	now          func() time.Time
	lastSignalAt time.Time
	stopOnce     sync.Once
}

func startCommandWatchdog(handle processguardian.Handle, firstTimeout, inactivityTimeout time.Duration) *commandWatchdog {
	wd := &commandWatchdog{
		signal: make(chan struct{}, 1),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		now:    time.Now,
	}

	go func() {
		defer close(wd.done)

		if firstTimeout <= 0 && inactivityTimeout <= 0 {
			return
		}

		firstEventSeen := false
		var firstTimer *time.Timer
		var firstTimerC <-chan time.Time
		if firstTimeout > 0 {
			firstTimer = time.NewTimer(firstTimeout)
			firstTimerC = firstTimer.C
		}

		var inactivityTimer *time.Timer
		var inactivityTimerC <-chan time.Time
		resetInactivity := func(delay time.Duration) {
			if inactivityTimeout <= 0 {
				return
			}
			if inactivityTimer == nil {
				inactivityTimer = time.NewTimer(delay)
				inactivityTimerC = inactivityTimer.C
				return
			}
			if !inactivityTimer.Stop() {
				select {
				case <-inactivityTimer.C:
				default:
				}
			}
			inactivityTimer.Reset(delay)
		}

		stopTimer := func(timer *time.Timer) {
			if timer == nil {
				return
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		defer stopTimer(firstTimer)
		defer stopTimer(inactivityTimer)

		for {
			select {
			case <-wd.stop:
				return
			case <-wd.signal:
				if !firstEventSeen {
					firstEventSeen = true
					if firstTimer != nil {
						stopTimer(firstTimer)
						firstTimer = nil
						firstTimerC = nil
					}
				}
				resetInactivity(wd.inactivityDelay(inactivityTimeout))
			case <-firstTimerC:
				wd.setErr(newProviderTimeoutError("first_event_timeout", firstTimeout))
				_ = handle.Cancel(context.Background())
				return
			case <-inactivityTimerC:
				// Signal coalesces notifications, so a newer observation can be
				// queued when the timer wins select readiness. Re-read it before
				// treating the command as inactive.
				if wd.resetInactivityAfterTimerFire(inactivityTimeout, resetInactivity) {
					continue
				}
				wd.setErr(newProviderTimeoutError("stream_inactivity_timeout", inactivityTimeout))
				_ = handle.Cancel(context.Background())
				return
			}
		}
	}()

	return wd
}

func (w *commandWatchdog) setErr(err error) {
	if err == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err == nil {
		w.err = err
	}
}

func (w *commandWatchdog) Signal() {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.lastSignalAt = w.now()
	w.mu.Unlock()
	select {
	case w.signal <- struct{}{}:
	default:
	}
}

func (w *commandWatchdog) signalAt() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastSignalAt
}

func (w *commandWatchdog) inactivityDelay(timeout time.Duration) time.Duration {
	return watchdogInactivityDelay(w.signalAt(), timeout, w.now())
}

func (w *commandWatchdog) resetInactivityAfterTimerFire(timeout time.Duration, reset func(time.Duration)) bool {
	delay := w.inactivityDelay(timeout)
	if delay <= 0 {
		return false
	}
	reset(delay)
	return true
}

func watchdogInactivityDelay(observedAt time.Time, timeout time.Duration, now time.Time) time.Duration {
	if delay := observedAt.Add(timeout).Sub(now); delay > 0 {
		return delay
	}
	return 0
}

func (w *commandWatchdog) Stop() {
	if w == nil {
		return
	}
	w.stopOnce.Do(func() { close(w.stop) })
	<-w.done
}

func (w *commandWatchdog) Err() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

type stepFinishTerminalClass string

const (
	stepFinishNonTerminal              stepFinishTerminalClass = "non_terminal"
	stepFinishTerminalSuccessCandidate stepFinishTerminalClass = "terminal_success_candidate"
	stepFinishTerminalFailure          stepFinishTerminalClass = "terminal_failure"
	stepFinishAmbiguous                stepFinishTerminalClass = "ambiguous"
)

func normalizeStepFinishReason(reason string) string {
	trimmed := strings.ToLower(strings.TrimSpace(reason))
	if trimmed == "" {
		return "unknown"
	}
	return trimmed
}

func classifyStepFinishReason(reason string) stepFinishTerminalClass {
	switch normalizeStepFinishReason(reason) {
	case "tool-calls":
		return stepFinishNonTerminal
	case "error":
		return stepFinishTerminalFailure
	case "stop":
		return stepFinishTerminalSuccessCandidate
	default:
		return stepFinishAmbiguous
	}
}

func newProviderTimeoutError(timeoutType string, timeout time.Duration) error {
	return &providerTimeoutError{
		TimeoutType: strings.TrimSpace(timeoutType),
		Timeout:     timeout,
	}
}

func resolveProviderInvocationID(req ExecuteRequest) string {
	if trimmed := strings.TrimSpace(req.InvocationID); trimmed != "" {
		return trimmed
	}
	return fmt.Sprintf("%s:%d", sanitizeProviderInvocationToken(req.AttemptID), time.Now().UTC().UnixNano())
}

func sanitizeProviderInvocationToken(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "invocation"
	}
	replacer := strings.NewReplacer(" ", "_", "\t", "_", "\n", "_", "\r", "_")
	return replacer.Replace(trimmed)
}

func buildProviderInvocationPromptMarker(invocationID string) string {
	trimmed := strings.TrimSpace(invocationID)
	if trimmed == "" {
		return ""
	}
	return providerInvocationIDLabel + " " + trimmed
}

func providerInvocationPromptParts(prompt, attemptID, invocationID string) []string {
	trimmedPrompt := strings.TrimRight(prompt, "\n")
	marker := buildProviderInvocationPromptMarker(invocationID)
	if marker == "" {
		return []string{trimmedPrompt}
	}
	metadataBlock := strings.Join([]string{
		providerInvocationMetadataHeader,
		providerInvocationAttemptLabel + " " + strings.TrimSpace(attemptID),
		marker,
	}, "\n")
	if trimmedPrompt == "" {
		return []string{metadataBlock}
	}
	return []string{trimmedPrompt, "\n\n", metadataBlock}
}

func isExecutionCancelled(err error) bool {
	return err != nil && errors.Is(err, context.Canceled)
}

func isExecutionTimedOut(err error) bool {
	return err != nil && errors.Is(err, context.DeadlineExceeded)
}

func firstNonNilError(errorsToCheck ...error) error {
	for _, err := range errorsToCheck {
		if err != nil {
			return err
		}
	}
	return nil
}

func buildTimeoutFailurePayload(timeoutErr error, diagnostics streamDiagnostics, execDiagnostics executionPathDiagnostics) map[string]any {
	payload := map[string]any{
		"error":                      timeoutErr.Error(),
		"stream_diagnostics":         diagnostics.toMap(),
		"execution_path_diagnostics": execDiagnostics.toMap(),
	}
	var typed *providerTimeoutError
	if errors.As(timeoutErr, &typed) {
		payload["timeout_type"] = strings.TrimSpace(typed.TimeoutType)
		payload["timeout_ms"] = typed.Timeout.Milliseconds()
	}
	return payload
}

func (d executionPathDiagnostics) toMap() map[string]any {
	payload := map[string]any{
		"attempt_id":                   strings.TrimSpace(d.AttemptID),
		"session_id":                   strings.TrimSpace(d.SessionID),
		"cmd_started_at":               strings.TrimSpace(d.CmdStartedAt),
		"cmd_wait_started_at":          strings.TrimSpace(d.CmdWaitStartedAt),
		"cmd_wait_finished_at":         strings.TrimSpace(d.CmdWaitFinishedAt),
		"wait_returned":                d.WaitReturned,
		"step_finish_seen":             d.StepFinishSeen,
		"last_step_finish_reason":      strings.TrimSpace(d.LastStepFinishReason),
		"last_step_finish_reason_kind": strings.TrimSpace(d.LastStepFinishReasonKind),
		"terminal_state_class":         strings.TrimSpace(d.TerminalStateClass),
		"upstream_error_text":          strings.TrimSpace(d.UpstreamErrorText),
		"completion_gate_passed":       d.CompletionGatePassed,
		"stdout_eof_seen":              d.StdoutEOFSeen,
		"stderr_eof_seen":              d.StderrEOFSeen,
		"stdout_event_count":           d.StdoutEventCount,
		"stdout_malformed_line_count":  d.StdoutMalformedLineCount,
		"stderr_line_count":            d.StderrLineCount,
		"last_stdout_event_at":         strings.TrimSpace(d.LastStdoutEventAt),
		"last_stderr_line_at":          strings.TrimSpace(d.LastStderrLineAt),
		"last_progress_text_len":       d.LastProgressTextLen,
		"last_progress_text_sha":       strings.TrimSpace(d.LastProgressTextSHA),
		"continuation_turns_elapsed":   d.ContinuationTurnsElapsed,
		"succeeded_event_appended":     d.SucceededEventAppended,
		"failed_event_appended":        d.FailedEventAppended,
		"cancelled_event_appended":     d.CancelledEventAppended,
	}
	if strings.TrimSpace(d.FirstStdoutMalformedAt) != "" {
		payload["first_stdout_malformed_at"] = strings.TrimSpace(d.FirstStdoutMalformedAt)
	}
	if strings.TrimSpace(d.LastStdoutMalformedAt) != "" {
		payload["last_stdout_malformed_at"] = strings.TrimSpace(d.LastStdoutMalformedAt)
	}
	if strings.TrimSpace(d.FirstStdoutMalformedSHA) != "" {
		payload["first_stdout_malformed_sha"] = strings.TrimSpace(d.FirstStdoutMalformedSHA)
	}
	if strings.TrimSpace(d.LastStdoutMalformedSHA) != "" {
		payload["last_stdout_malformed_sha"] = strings.TrimSpace(d.LastStdoutMalformedSHA)
	}
	if d.ExitCode != nil {
		payload["exit_code"] = *d.ExitCode
	}
	if d.ExitSignal != nil {
		payload["exit_signal"] = *d.ExitSignal
	}
	if strings.TrimSpace(d.WaitError) != "" {
		payload["wait_error"] = strings.TrimSpace(d.WaitError)
	}
	return payload
}

func (d executionPathDiagnostics) progressSnapshot() map[string]any {
	payload := map[string]any{
		"step_finish_seen":             d.StepFinishSeen,
		"last_step_finish_reason":      strings.TrimSpace(d.LastStepFinishReason),
		"last_step_finish_reason_kind": strings.TrimSpace(d.LastStepFinishReasonKind),
		"terminal_state_class":         strings.TrimSpace(d.TerminalStateClass),
		"completion_gate_passed":       d.CompletionGatePassed,
		"stdout_event_count":           d.StdoutEventCount,
		"stdout_malformed_line_count":  d.StdoutMalformedLineCount,
		"last_stdout_event_at":         strings.TrimSpace(d.LastStdoutEventAt),
		"last_progress_text_len":       d.LastProgressTextLen,
		"last_progress_text_sha":       strings.TrimSpace(d.LastProgressTextSHA),
		"cmd_started_at":               strings.TrimSpace(d.CmdStartedAt),
		"wait_returned":                d.WaitReturned,
		"continuation_turns_elapsed":   d.ContinuationTurnsElapsed,
		"succeeded_event_appended":     d.SucceededEventAppended,
		"failed_event_appended":        d.FailedEventAppended,
	}
	return payload
}

func recordMalformedStdoutLine(d *executionPathDiagnostics, line string) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		trimmed = "<blank>"
	}
	sum := sha256.Sum256([]byte(trimmed))
	recordMalformedStdoutDigest(d, fmt.Sprintf("%x", sum[:]))
}

func recordMalformedStdoutDigest(d *executionPathDiagnostics, hash string) {
	if d == nil {
		return
	}
	d.StdoutMalformedLineCount++
	now := time.Now().UTC().Format(time.RFC3339)
	if strings.TrimSpace(d.FirstStdoutMalformedAt) == "" {
		d.FirstStdoutMalformedAt = now
	}
	d.LastStdoutMalformedAt = now
	if strings.TrimSpace(d.FirstStdoutMalformedSHA) == "" {
		d.FirstStdoutMalformedSHA = hash
	}
	d.LastStdoutMalformedSHA = hash
}

func decorateTimeoutError(timeoutErr error, diagnostics streamDiagnostics) error {
	if timeoutErr == nil {
		return nil
	}
	return fmt.Errorf("%w (first_event_latency_ms=%d max_inter_event_gap_ms=%d event_count=%d)",
		timeoutErr,
		diagnostics.FirstEventLatency.Milliseconds(),
		diagnostics.MaxInterEventGap.Milliseconds(),
		diagnostics.EventCount,
	)
}

func (d streamDiagnostics) toMap() map[string]any {
	payload := map[string]any{
		"first_event_latency_ms": d.FirstEventLatency.Milliseconds(),
		"max_inter_event_gap_ms": d.MaxInterEventGap.Milliseconds(),
		"event_count":            d.EventCount,
	}
	if strings.TrimSpace(d.ProcessStartedAt) != "" {
		payload["process_started_at"] = strings.TrimSpace(d.ProcessStartedAt)
	}
	if strings.TrimSpace(d.FirstEventAt) != "" {
		payload["first_event_at"] = strings.TrimSpace(d.FirstEventAt)
	}
	if strings.TrimSpace(d.LastEventAt) != "" {
		payload["last_event_at"] = strings.TrimSpace(d.LastEventAt)
	}
	if strings.TrimSpace(d.ProcessExitedAt) != "" {
		payload["process_exited_at"] = strings.TrimSpace(d.ProcessExitedAt)
	}
	return payload
}
