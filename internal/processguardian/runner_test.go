//go:build darwin || linux

package processguardian

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if exitCode, handled := runStdinTestProvider(os.Args); handled {
		os.Exit(exitCode)
	}
	if len(os.Args) > 1 && os.Args[1] == HiddenGuardianMode {
		switch os.Getenv("KINGDOM_TEST_GUARDIAN_BLOCK") {
		case "ownership":
			os.Exit(runBlockedOwnershipGuardian())
		case "started":
			os.Exit(runBlockedStartedGuardian())
		}
	}
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

func TestRunnerStartHonorsContextWhileOwnershipAcknowledgementIsBlocked(t *testing.T) {
	runtimeRoot := t.TempDir()
	marker := filepath.Join(t.TempDir(), "gate.pid")
	t.Setenv("KINGDOM_TEST_GUARDIAN_BLOCK", "ownership")
	t.Setenv("KINGDOM_TEST_GUARDIAN_MARKER", marker)
	runner, err := NewRunner(Config{Executable: os.Args[0], RuntimeRoot: runtimeRoot, CancelGrace: 100 * time.Millisecond, CancelForce: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startDone := make(chan error, 1)
	go func() {
		_, startErr := runner.Start(ctx, shellCommand("true"), func(context.Context, Ownership) error {
			return errors.New("persistence must not run before ownership acknowledgement")
		})
		startDone <- startErr
	}()
	pid := readGuardianTestPID(t, marker)
	startedAt := time.Now()
	cancel()
	select {
	case err := <-startDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected blocked ownership acknowledgement to return context cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked ownership acknowledgement did not return after cancellation")
	}
	if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
		t.Fatalf("blocked ownership acknowledgement ignored context cancellation: %s", elapsed)
	}
	if err := waitForGroupAbsentForTest(pid, time.Second); err != nil {
		t.Fatalf("unacknowledged gate remained after cancellation: %v", err)
	}
}

func TestRunnerStartCancelsAcknowledgedTreeWhileStartedAcknowledgementIsBlocked(t *testing.T) {
	runtimeRoot := t.TempDir()
	marker := filepath.Join(t.TempDir(), "released")
	t.Setenv("KINGDOM_TEST_GUARDIAN_BLOCK", "started")
	t.Setenv("KINGDOM_TEST_GUARDIAN_MARKER", marker)
	runner, err := NewRunner(Config{Executable: os.Args[0], RuntimeRoot: runtimeRoot, CancelGrace: 100 * time.Millisecond, CancelForce: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var ownership Ownership
	_, err = runner.Start(ctx, shellCommand("true"), func(_ context.Context, got Ownership) error {
		ownership = got
		cancel() // Simulate cancellation immediately after the durable commit.
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected blocked started acknowledgement to return context cancellation, got %v", err)
	}
	var launchErr *LaunchError
	if !errors.As(err, &launchErr) || !launchErr.Result.Quiescent || launchErr.Result.EvidenceSHA256 == "" || launchErr.Result.EvidenceJSON == "" {
		t.Fatalf("expected durable acknowledged launch quiescence, got %#v", err)
	}
	evidence, digest, raw, readErr := ReadQuiescenceEvidence(runtimeRoot, ownership.GuardID)
	if readErr != nil || digest != launchErr.Result.EvidenceSHA256 || raw != launchErr.Result.EvidenceJSON || evidence.QuiescenceCapability != ownership.QuiescenceCapability {
		t.Fatalf("read durable aborted-launch evidence: result=%+v digest=%q err=%v", evidence, digest, readErr)
	}
	if ownership.TreeID <= 0 {
		t.Fatalf("expected persisted ownership, got %+v", ownership)
	}
	if err := waitForGroupAbsentForTest(ownership.TreeID, time.Second); err != nil {
		t.Fatalf("acknowledged process tree remained after cancellation: %v", err)
	}
}

func TestStatusReaderShutdownReleasesRepeatedCancelledLaunches(t *testing.T) {
	for attempt := 0; attempt < 32; attempt++ {
		status, writer, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		reader := startStatusReader(status)
		if err := writeFrame(writer, wireStatus{Type: "ownership"}); err != nil {
			_ = writer.Close()
			t.Fatal(err)
		}
		deadline := time.Now().Add(time.Second)
		for len(reader.updates) != 1 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if len(reader.updates) != 1 {
			_ = writer.Close()
			reader.shutdown()
			t.Fatal("ownership update did not fill the unread status buffer")
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() {
			reader.shutdown()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("status reader remained blocked behind a buffered ownership update")
		}
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

func readGuardianTestPID(t *testing.T, path string) int {
	t.Helper()
	if err := waitForFile(path, time.Second); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 0 {
		t.Fatalf("invalid guardian test pid %q: %v", raw, err)
	}
	return pid
}

func waitForGroupAbsentForTest(treeID int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if groupAbsent(treeID) {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("process group %d remained alive", treeID)
}

func runBlockedOwnershipGuardian() int {
	spec, control, status, stdout, stderr, stdin, err := guardianFiles()
	if err != nil {
		return 1
	}
	defer closeFiles(spec, control, status, stdout, stderr, stdin)
	var launch wireSpec
	if readFrame(spec, &launch) != nil {
		return 1
	}
	releaseRead, releaseWrite, err := os.Pipe()
	if err != nil {
		return 1
	}
	defer releaseWrite.Close()
	gate := exec.Command("/bin/sh", "-c", "read ignored")
	gate.Stdin = releaseRead
	gate.Stdout = stdout
	gate.Stderr = stderr
	gate.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := gate.Start(); err != nil {
		return 1
	}
	_ = releaseRead.Close()
	_ = os.WriteFile(os.Getenv("KINGDOM_TEST_GUARDIAN_MARKER"), []byte(strconv.Itoa(gate.Process.Pid)), 0o600)
	for {
		time.Sleep(time.Hour)
	}
}

func runBlockedStartedGuardian() int {
	spec, control, status, stdout, stderr, stdin, err := guardianFiles()
	if err != nil {
		return 1
	}
	defer closeFiles(spec, control, status, stdout, stderr, stdin)
	var launch wireSpec
	if readFrame(spec, &launch) != nil {
		return 1
	}
	provider := exec.Command("/bin/sh", "-c", "sleep 60")
	provider.Stdout = stdout
	provider.Stderr = stderr
	provider.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := provider.Start(); err != nil {
		return 1
	}
	identity, err := processIdentityToken(provider.Process.Pid)
	if err != nil {
		return 1
	}
	ownership := Ownership{GuardID: launch.GuardID, GuardianPID: os.Getpid(), ChildPID: provider.Process.Pid, ChildIdentityToken: identity, TreeID: provider.Process.Pid, TreeIdentityToken: identity, Platform: "unix_process_group", QuiescenceCapability: launch.Capability}
	if writeFrame(status, wireStatus{Type: "ownership", Ownership: ownership}) != nil {
		return 1
	}
	command, err := readControl(control)
	if err != nil || (command != 'R' && command != 'D') {
		return 1
	}
	_ = os.WriteFile(os.Getenv("KINGDOM_TEST_GUARDIAN_MARKER"), []byte("released"), 0o600)
	_, _ = readControl(control)
	_ = provider.Wait()
	result := Result{Ownership: ownership, Forced: true, Quiescent: groupAbsent(ownership.TreeID), FailureCode: "provider_cancelled", FailureText: "guardian launch was cancelled"}
	if command == 'D' {
		launch.Durable = true
		result.EvidenceSHA256, result.EvidenceJSON, _ = publishEvidence(launch, result)
	}
	_ = writeFrame(status, wireStatus{Type: "result", Result: result})
	return 0
}
