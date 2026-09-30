//go:build darwin || linux || windows

package processguardian

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	stdinEchoTestProvider      = "--kingdom-processguardian-test-stdin-echo"
	stdinCloseWaitTestProvider = "--kingdom-processguardian-test-stdin-close-wait"
	stdinIdleTestProvider      = "--kingdom-processguardian-test-stdin-idle"
)

type stdinReadResult struct {
	data []byte
	err  error
}

func runStdinTestProvider(args []string) (int, bool) {
	if len(args) < 2 {
		return 0, false
	}
	switch args[1] {
	case stdinEchoTestProvider:
		if _, err := io.Copy(os.Stdout, os.Stdin); err != nil {
			return 1, true
		}
		return 0, true
	case stdinCloseWaitTestProvider:
		if len(args) != 3 || os.Stdin.Close() != nil {
			return 1, true
		}
		if err := os.WriteFile(args[2], []byte("closed"), 0o600); err != nil {
			return 1, true
		}
		time.Sleep(time.Minute)
		return 0, true
	case stdinIdleTestProvider:
		time.Sleep(time.Minute)
		return 0, true
	default:
		return 0, false
	}
}

func TestRunnerStreamsLargeStdinOutsideBoundedWire(t *testing.T) {
	runner := newStdinTestRunner(t)
	handle := startStdinTestChild(t, runner, stdinEchoTestProvider, nil, nil)

	payload := make([]byte, (4<<20)+(128<<10))
	if len(payload) <= maxWireBytes {
		t.Fatalf("stdin test payload is not larger than the wire-frame limit: payload=%d limit=%d", len(payload), maxWireBytes)
	}
	for index := range payload {
		payload[index] = byte((index*31 + 17) % 251)
	}

	stdoutDone := make(chan stdinReadResult, 1)
	stderrDone := make(chan stdinReadResult, 1)
	go func() {
		data, err := io.ReadAll(handle.Stdout())
		stdoutDone <- stdinReadResult{data: data, err: err}
	}()
	go func() {
		data, err := io.ReadAll(handle.Stderr())
		stderrDone <- stdinReadResult{data: data, err: err}
	}()

	written, err := io.Copy(handle.Stdin(), bytes.NewReader(payload))
	if err != nil || written != int64(len(payload)) {
		t.Fatalf("write child stdin: wrote %d of %d bytes, err=%v", written, len(payload), err)
	}
	if err := handle.Stdin().Close(); err != nil {
		t.Fatalf("close child stdin: %v", err)
	}
	if err := handle.Stdin().Close(); err != nil {
		t.Fatalf("repeat child stdin close: %v", err)
	}
	if _, err := handle.Stdin().Write([]byte("after EOF")); err == nil {
		t.Fatal("write after child stdin EOF succeeded")
	}

	stdout := receiveStdinRead(t, stdoutDone, "stdout")
	if !bytes.Equal(stdout, payload) {
		t.Fatalf("child stdin round-trip differs: got %d bytes, want %d", len(stdout), len(payload))
	}
	if stderr := receiveStdinRead(t, stderrDone, "stderr"); len(stderr) != 0 {
		t.Fatalf("unexpected child stderr: %q", stderr)
	}
	result, err := waitStdinHandle(t, handle)
	if err != nil {
		t.Fatalf("wait for stdin echo child: %v", err)
	}
	if !result.Quiescent || result.Forced || result.ExitCode != 0 {
		t.Fatalf("unexpected stdin echo result: %+v", result)
	}
}

func TestControlMetadataFramesRemainBounded(t *testing.T) {
	var frame bytes.Buffer
	spec := wireSpec{
		Version:    "v1",
		GuardID:    strings.Repeat("g", 32),
		Capability: strings.Repeat("c", 64),
		Command:    Command{Path: "provider", Args: []string{strings.Repeat("x", maxWireBytes)}},
	}
	if err := writeFrame(&frame, spec); err == nil {
		t.Fatal("oversized control metadata frame was accepted")
	}
	if frame.Len() != 0 {
		t.Fatalf("oversized control metadata emitted a partial frame: %d bytes", frame.Len())
	}
}

func TestRunnerCancelsProcessWhenChildStdinWriteFails(t *testing.T) {
	runner := newStdinTestRunner(t)
	closedMarker := filepath.Join(t.TempDir(), "stdin-closed")
	var ownership Ownership
	handle := startStdinTestChild(t, runner, stdinCloseWaitTestProvider, []string{closedMarker}, func(_ context.Context, got Ownership) error {
		ownership = got
		return nil
	})
	if err := waitForStdinMarker(closedMarker, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Stdin().Write([]byte("write after child closed stdin")); err == nil {
		t.Fatal("write to closed child stdin succeeded")
	}
	result, err := waitStdinHandle(t, handle)
	if err != nil {
		t.Fatalf("wait after child stdin write failure: %v", err)
	}
	assertStdinTreeQuiescent(t, result, ownership)
}

func TestRunnerCancellationUnblocksStdinWriteAndQuiescesTree(t *testing.T) {
	runner := newStdinTestRunner(t)
	var ownership Ownership
	handle := startStdinTestChild(t, runner, stdinIdleTestProvider, nil, func(_ context.Context, got Ownership) error {
		ownership = got
		return nil
	})
	writeDone := make(chan error, 1)
	go func() {
		_, err := handle.Stdin().Write(make([]byte, 16<<20))
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		t.Fatalf("large stdin write unexpectedly completed before cancellation: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := handle.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel guarded child: %v", err)
	}
	select {
	case err := <-writeDone:
		if err == nil {
			t.Fatal("blocked stdin write succeeded after cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not unblock child stdin write")
	}
	result, err := waitStdinHandle(t, handle)
	if err != nil {
		t.Fatalf("wait after stdin write cancellation: %v", err)
	}
	assertStdinTreeQuiescent(t, result, ownership)
}

func newStdinTestRunner(t *testing.T) *Runner {
	t.Helper()
	runner, err := NewRunner(Config{RuntimeRoot: t.TempDir(), CancelGrace: 100 * time.Millisecond, CancelForce: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func startStdinTestChild(t *testing.T, runner *Runner, mode string, extraArgs []string, persist PersistOwnership) Handle {
	t.Helper()
	args := append([]string{mode}, extraArgs...)
	handle, err := runner.Start(context.Background(), Command{Path: os.Args[0], Args: args, Env: os.Environ()}, persist)
	if err != nil {
		t.Fatalf("start stdin test child: %v", err)
	}
	t.Cleanup(func() {
		_ = handle.Cancel(context.Background())
		_ = handle.Stdin().Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = handle.Wait(ctx)
	})
	return handle
}

func receiveStdinRead(t *testing.T, done <-chan stdinReadResult, stream string) []byte {
	t.Helper()
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatalf("read child %s: %v", stream, result.err)
		}
		return result.data
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out reading child %s", stream)
		return nil
	}
}

func waitStdinHandle(t *testing.T, handle Handle) (Result, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return handle.Wait(ctx)
}

func assertStdinTreeQuiescent(t *testing.T, result Result, ownership Ownership) {
	t.Helper()
	if !result.Quiescent || !result.Forced {
		t.Fatalf("expected forced quiescence after stdin failure/cancellation, got %+v", result)
	}
	if ownership.ChildPID <= 0 || !ManagedProcessAbsent(ownership.ChildPID, ownership.TreeID, ownership.ChildIdentityToken, ownership.TreeIdentityToken) {
		t.Fatalf("guarded child tree remained after stdin failure/cancellation: %+v", ownership)
	}
}

func waitForStdinMarker(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return context.DeadlineExceeded
}
