//go:build darwin || linux

package processguardian

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if exitCode, handled := DispatchHiddenMode(os.Args); handled {
		os.Exit(exitCode)
	}
	os.Exit(m.Run())
}

func TestRunnerPersistsOwnershipBeforeRelease(t *testing.T) {
	t.Parallel()
	runner, err := NewRunner(Config{RuntimeRoot: t.TempDir(), CancelGrace: time.Second, CancelForce: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "provider-ran")
	persisted := false
	handle, err := runner.Start(context.Background(), shellCommand("echo provider > "+shellQuote(marker)), func(_ context.Context, ownership Ownership) error {
		if ownership.GuardID == "" || ownership.ChildPID <= 0 || ownership.TreeID != ownership.ChildPID {
			t.Fatalf("invalid ownership: %+v", ownership)
		}
		if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("provider escaped launch gate before persistence: %v", err)
		}
		persisted = true
		return nil
	})
	if err != nil {
		t.Fatalf("start guarded command: %v", err)
	}
	if !persisted {
		t.Fatal("ownership was not persisted")
	}
	if _, err := io.ReadAll(handle.Stdout()); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(handle.Stderr()); err != nil {
		t.Fatal(err)
	}
	result, err := handle.Wait(context.Background())
	if err != nil {
		t.Fatalf("wait guarded command: %v", err)
	}
	if !result.Quiescent || result.Forced || result.ExitCode != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("provider did not run after release: %v", err)
	}
}

func TestRunnerPersistenceFailureNeverReleasesProvider(t *testing.T) {
	t.Parallel()
	runner, err := NewRunner(Config{RuntimeRoot: t.TempDir(), CancelGrace: time.Second, CancelForce: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "provider-ran")
	_, err = runner.Start(context.Background(), shellCommand("echo provider > "+shellQuote(marker)), func(context.Context, Ownership) error { return errors.New("sqlite unavailable") })
	if err == nil || !strings.Contains(err.Error(), "persist guardian ownership") {
		t.Fatalf("expected persistence error, got %v", err)
	}
	var launchErr *LaunchError
	if !errors.As(err, &launchErr) || !launchErr.Result.Quiescent || launchErr.Result.EvidenceSHA256 != "" || launchErr.Result.EvidenceJSON != "" {
		t.Fatalf("expected non-durable aborted launch quiescence, got %#v", err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("provider escaped failed launch gate: %v", err)
	}
}

func TestRunnerCancellationKillsDescendantsAndPublishesEvidence(t *testing.T) {
	t.Parallel()
	runtimeRoot := t.TempDir()
	runner, err := NewRunner(Config{RuntimeRoot: runtimeRoot, CancelGrace: 100 * time.Millisecond, CancelForce: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	childPIDFile := filepath.Join(t.TempDir(), "child.pid")
	handle, err := runner.Start(context.Background(), shellCommand("sleep 60 & echo $! > "+shellQuote(childPIDFile)+"; wait"), func(context.Context, Ownership) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := waitForFile(childPIDFile, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := handle.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(handle.Stderr()); err != nil {
		t.Fatal(err)
	}
	result, err := handle.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Quiescent || !result.Forced {
		t.Fatalf("expected forced quiescence, got %+v", result)
	}
	if result.EvidenceSHA256 == "" {
		t.Fatal("expected durable quiescence evidence")
	}
	evidence := filepath.Join(runtimeRoot, "provider-process-guards", "quiescence")
	entries, err := os.ReadDir(evidence)
	if err != nil || len(entries) != 1 {
		t.Fatalf("expected one evidence record: entries=%d err=%v", len(entries), err)
	}
}

func TestRunnerGuardianDeathTerminatesAcknowledgedTree(t *testing.T) {
	t.Parallel()
	runner, err := NewRunner(Config{RuntimeRoot: t.TempDir(), CancelGrace: 100 * time.Millisecond, CancelForce: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var ownership Ownership
	handle, err := runner.Start(context.Background(), shellCommand("sleep 60"), func(_ context.Context, got Ownership) error { ownership = got; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if ownership.GuardianPID <= 0 || ownership.ChildPID <= 0 {
		t.Fatalf("invalid acknowledged ownership: %+v", ownership)
	}
	if err := syscall.Kill(ownership.GuardianPID, syscall.SIGKILL); err != nil {
		t.Fatalf("kill guardian: %v", err)
	}
	if _, err := io.ReadAll(handle.Stdout()); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(handle.Stderr()); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Wait(context.Background()); err == nil {
		t.Fatal("expected guardian death to fail the handle")
	}
	if err := syscall.Kill(-ownership.TreeID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("guardian death left process group alive: %v", err)
	}
}

func TestRunnerOwnerPipeEOFTerminatesContinuouslyWritingCommand(t *testing.T) {
	t.Parallel()
	runner, err := NewRunner(Config{RuntimeRoot: t.TempDir(), CancelGrace: 100 * time.Millisecond, CancelForce: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	h, err := runner.Start(context.Background(), shellCommand("while :; do printf x; sleep 0.01; done"), nil)
	if err != nil {
		t.Fatal(err)
	}
	internal, ok := h.(*handle)
	if !ok {
		t.Fatal("expected native guardian handle")
	}
	if err := internal.control.Close(); err != nil {
		t.Fatalf("close owner control pipe: %v", err)
	}
	if _, err := io.ReadAll(h.Stdout()); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(h.Stderr()); err != nil {
		t.Fatal(err)
	}
	result, err := h.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Quiescent || !result.Forced || result.FailureCode != "provider_cancelled" {
		t.Fatalf("unexpected owner-EOF result: %+v", result)
	}
}

func TestQuiescenceEvidenceReplayUsesPinnedIdentity(t *testing.T) {
	t.Parallel()
	runtimeRoot := t.TempDir()
	runner, err := NewRunner(Config{RuntimeRoot: runtimeRoot, CancelGrace: time.Second, CancelForce: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := runner.Start(context.Background(), shellCommand("true"), func(context.Context, Ownership) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(handle.Stdout()); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(handle.Stderr()); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, digest, evidence, err := ReadQuiescenceEvidence(runtimeRoot, handle.GuardID())
	if err != nil || result.GuardID != handle.GuardID() || digest == "" || evidence == "" {
		t.Fatalf("read quiescence evidence: result=%+v digest=%q err=%v", result, digest, err)
	}
	if err := RemoveQuiescenceEvidence(runtimeRoot, handle.GuardID(), evidence); err != nil {
		t.Fatalf("remove exact quiescence evidence: %v", err)
	}
	if _, _, _, err := ReadQuiescenceEvidence(runtimeRoot, handle.GuardID()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected evidence removal, got %v", err)
	}
}

func TestWireRejectsUnknownAndOversizedFrames(t *testing.T) {
	t.Parallel()
	var decoded wireSpec
	if err := readFrame(strings.NewReader("\x00\x01{}"), &decoded); err == nil {
		t.Fatal("accepted malformed frame")
	}
}

func shellCommand(script string) Command {
	return Command{Path: "/bin/sh", Args: []string{"-c", script}, Env: os.Environ()}
}

func shellQuote(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'" }

func waitForFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("timed out waiting for provider test marker")
}
