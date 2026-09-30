//go:build darwin || linux || windows

package cli

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"kingdom/internal/processguardian"
)

func TestCLITestExecutableDispatchesGuardianBeforeTestFlags(t *testing.T) {
	runner, err := processguardian.NewRunner(processguardian.Config{RuntimeRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	owned := false
	handle, err := runner.Start(ctx, processguardian.Command{Path: executable, Args: []string{"-test.run=^$"}}, func(_ context.Context, ownership processguardian.Ownership) error {
		owned = ownership.GuardID != "" && ownership.ChildPID > 0 && ownership.GuardianPID > 0
		return nil
	})
	if err != nil {
		t.Fatalf("start guarded CLI test executable: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = handle.Cancel(cleanupCtx)
		_, _ = handle.Wait(cleanupCtx)
	})
	if err := handle.Stdin().Close(); err != nil {
		t.Fatal(err)
	}
	stdout, stdoutErr := io.ReadAll(handle.Stdout())
	stderr, stderrErr := io.ReadAll(handle.Stderr())
	result, waitErr := handle.Wait(ctx)
	if stdoutErr != nil || stderrErr != nil || waitErr != nil {
		t.Fatalf("drain guarded test executable: stdout=%v stderr=%v wait=%v", stdoutErr, stderrErr, waitErr)
	}
	if !owned || !result.Quiescent || result.ExitCode != 0 || !strings.Contains(string(stdout), "PASS") {
		t.Fatalf("guarded test execution failed: owned=%t result=%+v stdout=%q stderr=%q", owned, result, stdout, stderr)
	}
}
