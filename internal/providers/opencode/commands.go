package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/processguardian"
)

const (
	defaultCommandTimeoutMillis = 120_000
	commandOutputLimit          = 64 << 10
	commandCancelLimit          = 10 * time.Second
)

// commandInput is the plugin's report of one managed shell tool call.
type commandInput struct {
	Identity  providerexecution.RequestIdentity `json:"identity"`
	MessageID string                            `json:"messageID"`
	CallID    string                            `json:"callID"`
	Command   string                            `json:"command"`
	Cwd       string                            `json:"cwd"`
	Timeout   int64                             `json:"timeout"`
	ToolInput json.RawMessage                   `json:"toolInput"`
}

type commandResult struct {
	Output    string `json:"output"`
	Exit      int    `json:"exit"`
	GuardID   string `json:"guardID"`
	Quiescent bool   `json:"quiescent"`
	Truncated bool   `json:"truncated"`
}

// commandRegistration binds a request to the canonical callbacks that persist
// its command guardians.
type commandRegistration struct {
	workspace  string
	ownership  func(context.Context, CommandGuardianOwnership) error
	quiescence func(context.Context, CommandGuardianQuiescence) error
	calls      map[string]*commandCall
}

type commandCall struct {
	done   chan struct{}
	cancel context.CancelFunc
	result commandResult
	err    error
	// resolved means the claim is durably closed against the command's
	// quiescent guardian, whatever the command's own outcome was.
	resolved bool
}

// commandBridge runs managed shell calls under Kingdom's process guardian. A
// command is claimed durably before anything starts, and a claim whose outcome
// is unknown after a restart is never run again.
type commandBridge struct {
	authority *providerrequest.Authority
	local     LocalExecutor
	env       []string

	mu            sync.Mutex
	registrations map[providerexecution.RequestIdentity]*commandRegistration
}

func newCommandBridge(authority *providerrequest.Authority, local LocalExecutor, env []string) *commandBridge {
	return &commandBridge{authority: authority, local: local, env: commandEnvironment(env), registrations: map[providerexecution.RequestIdentity]*commandRegistration{}}
}

// commandEnvironment keeps provider and build configuration but drops the
// variables that configure or authenticate Kingdom's own OpenCode server.
func commandEnvironment(inherited []string) []string {
	env := make([]string, 0, len(inherited))
	for _, entry := range inherited {
		if !strings.HasPrefix(entry, "OPENCODE_") && !strings.HasPrefix(entry, "KINGDOM_") {
			env = append(env, entry)
		}
	}
	return env
}

func (b *commandBridge) register(identity providerexecution.RequestIdentity, registration *commandRegistration) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.registrations[identity]; exists {
		return fmt.Errorf("command request is already registered")
	}
	registration.calls = map[string]*commandCall{}
	b.registrations[identity] = registration
	return nil
}

func (b *commandBridge) unregister(identity providerexecution.RequestIdentity) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.registrations, identity)
}

// invoke claims the exact tool call, then runs it once. Replays join the
// running call or return its recorded result.
func (b *commandBridge) invoke(ctx context.Context, in commandInput) (commandResult, error) {
	if in.MessageID == "" || in.CallID == "" || strings.TrimSpace(in.Command) == "" || in.Timeout < 0 || in.Timeout > int64(time.Duration(1<<63-1)/time.Millisecond) {
		return commandResult{}, fmt.Errorf("invalid managed command")
	}
	b.mu.Lock()
	registration, ok := b.registrations[in.Identity]
	if !ok {
		b.mu.Unlock()
		return commandResult{}, fmt.Errorf("command request is not registered")
	}
	directory, err := b.local.ResolveCommandDirectory(registration.workspace, in.Cwd)
	if err != nil {
		b.mu.Unlock()
		return commandResult{}, err
	}
	if err := b.requireShellInput(in, registration.workspace, directory); err != nil {
		b.mu.Unlock()
		return commandResult{}, err
	}
	in.Cwd = directory
	// Claiming under the lock keeps a concurrent replay from observing a claim
	// that has no running call yet.
	claim, err := b.authority.ClaimCommand(ctx, providerrequest.NativeCall{Identity: in.Identity, MessageID: in.MessageID, CallID: in.CallID, Tool: "shell", Input: in.ToolInput})
	if err != nil {
		b.mu.Unlock()
		return commandResult{}, err
	}
	key := in.MessageID + "\x00" + in.CallID
	call := registration.calls[key]
	if !claim.Execute {
		b.mu.Unlock()
		if call == nil {
			return commandResult{}, fmt.Errorf("command outcome is unknown after a restart and will not run again")
		}
		return awaitCommand(ctx, call)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	call = &commandCall{done: make(chan struct{}), cancel: cancel}
	registration.calls[key] = call
	b.mu.Unlock()
	go b.execute(runCtx, in, claim, registration, call)
	return awaitCommand(ctx, call)
}

// requireShellInput ensures the guarded command is exactly what the stock tool
// call asked for, so the projected tool and the durable claim stay comparable.
func (b *commandBridge) requireShellInput(in commandInput, workspace, directory string) error {
	var input struct {
		Command    string `json:"command"`
		Workdir    string `json:"workdir"`
		Timeout    *int64 `json:"timeout"`
		Background bool   `json:"background"`
	}
	if err := json.Unmarshal(in.ToolInput, &input); err != nil {
		return fmt.Errorf("managed command input is invalid")
	}
	timeout := int64(defaultCommandTimeoutMillis)
	if input.Timeout != nil {
		timeout = *input.Timeout
	}
	workdir, err := b.local.ResolveCommandDirectory(workspace, input.Workdir)
	if err != nil {
		return err
	}
	if input.Background || input.Command != in.Command || timeout != in.Timeout || workdir != directory {
		return fmt.Errorf("managed command differs from its tool input")
	}
	return nil
}

func awaitCommand(ctx context.Context, call *commandCall) (commandResult, error) {
	select {
	case <-call.done:
		return call.result, call.err
	case <-ctx.Done():
		return commandResult{}, ctx.Err()
	}
}

func (b *commandBridge) execute(ctx context.Context, in commandInput, claim providerrequest.CommandClaim, registration *commandRegistration, call *commandCall) {
	defer close(call.done)
	defer call.cancel()
	if in.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(in.Timeout)*time.Millisecond)
		defer cancel()
	}
	ordinal := int(claim.Ordinal)
	started := time.Now().UTC().Format(time.RFC3339Nano)
	handle, err := b.local.Start(ctx, shellCommand(in.Command, in.Cwd, b.env), func(ctx context.Context, ownership processguardian.Ownership) error {
		return registration.ownership(ctx, CommandGuardianOwnership{CommandOrdinal: ordinal, StartedAt: started, AcknowledgedAt: time.Now().UTC().Format(time.RFC3339Nano), Ownership: ownership})
	})
	var result processguardian.Result
	if err == nil {
		call.result.Output, call.result.Truncated, result, err = superviseCommand(ctx, handle)
	} else {
		var launch *processguardian.LaunchError
		if errors.As(err, &launch) && launch.OwnershipPersisted {
			result = launch.Result
		} else {
			// The guardian stops its tree before releasing the command whenever
			// its ownership was not acknowledged and persisted, so the command
			// never ran. The claim closes only if no guardian row exists, which
			// excludes a commit whose outcome was merely reported as an error.
			if abortErr := b.authority.AbortCommand(context.Background(), in.Identity, claim.ID, claim.Digest); abortErr != nil {
				err = errors.Join(err, fmt.Errorf("abort command claim: %w", abortErr))
			} else {
				call.resolved = true
			}
			call.err = err
			return
		}
	}
	call.result.Exit, call.result.GuardID, call.result.Quiescent = result.ExitCode, result.GuardID, result.Quiescent
	if result.GuardID == "" || !result.Quiescent {
		call.err = errors.Join(err, fmt.Errorf("command quiescence is unproved"))
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if ingestErr := registration.quiescence(context.Background(), CommandGuardianQuiescence{CommandOrdinal: ordinal, ExitedAt: now, QuiescentAt: now, Result: result}); ingestErr != nil {
		err = errors.Join(err, fmt.Errorf("record command quiescence: %w", ingestErr))
	} else if completeErr := b.authority.CompleteCommand(context.Background(), in.Identity, claim.ID, claim.Digest, result.GuardID); completeErr != nil {
		err = errors.Join(err, fmt.Errorf("complete command claim: %w", completeErr))
	} else {
		call.resolved = true
	}
	call.err = err
}

// shellCommand runs the model's command line with the platform shell.
func shellCommand(line, directory string, env []string) processguardian.Command {
	if runtime.GOOS == "windows" {
		return processguardian.Command{Path: "cmd.exe", Args: []string{"/C", line}, Dir: directory, Env: env}
	}
	return processguardian.Command{Path: "/bin/sh", Args: []string{"-c", line}, Dir: directory, Env: env}
}

// superviseCommand drains bounded output and cancels the command tree when its
// context ends, returning only after the guardian reports a result.
func superviseCommand(ctx context.Context, handle processguardian.Handle) (string, bool, processguardian.Result, error) {
	_ = handle.Stdin().Close()
	finished := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			cancelCtx, cancel := context.WithTimeout(context.Background(), commandCancelLimit)
			defer cancel()
			_ = handle.Cancel(cancelCtx)
		case <-finished:
		}
	}()
	var stdout, stderr []byte
	var stdoutTruncated, stderrTruncated bool
	var drains sync.WaitGroup
	drains.Add(2)
	drain := func(reader io.ReadCloser, target *[]byte, truncated *bool) {
		defer drains.Done()
		defer reader.Close()
		*target, _ = io.ReadAll(io.LimitReader(reader, commandOutputLimit))
		discarded, _ := io.Copy(io.Discard, reader)
		*truncated = discarded > 0
	}
	go drain(handle.Stdout(), &stdout, &stdoutTruncated)
	go drain(handle.Stderr(), &stderr, &stderrTruncated)
	result, err := handle.Wait(context.Background())
	close(finished)
	drains.Wait()
	return string(stdout) + string(stderr), stdoutTruncated || stderrTruncated, result, err
}

// cancel closes the request's admission, then stops and joins every command it
// has already claimed. A command that ends because of the cancellation is
// resolved; one whose guardian closure is unproved is reported and stays as
// resource debt for settlement.
func (b *commandBridge) cancel(ctx context.Context, identity providerexecution.RequestIdentity) error {
	if err := b.authority.CloseAdmission(ctx, identity); err != nil {
		return err
	}
	b.mu.Lock()
	var calls []*commandCall
	if registration, ok := b.registrations[identity]; ok {
		for _, call := range registration.calls {
			call.cancel()
			calls = append(calls, call)
		}
	}
	b.mu.Unlock()
	var unresolved []error
	for _, call := range calls {
		select {
		case <-call.done:
			if !call.resolved {
				unresolved = append(unresolved, call.err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.Join(unresolved...)
}
