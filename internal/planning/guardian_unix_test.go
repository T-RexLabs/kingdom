//go:build darwin || linux

package planning

import (
	"context"
	"io"
	"testing"

	"kingdom/internal/processguardian"
)

func TestPlanningTestBinaryRunsGuardedCommand(t *testing.T) {
	runner, err := processguardian.NewRunner(processguardian.Config{RuntimeRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := runner.Start(t.Context(), processguardian.Command{Path: "/usr/bin/true"}, nil)
	if err != nil {
		t.Fatalf("start guarded command: %v", err)
	}
	if _, err := io.ReadAll(handle.Stdout()); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if _, err := io.ReadAll(handle.Stderr()); err != nil {
		t.Fatalf("read stderr: %v", err)
	}
	result, err := handle.Wait(context.Background())
	if err != nil {
		t.Fatalf("wait guarded command: %v", err)
	}
	if !result.Quiescent || result.ExitCode != 0 {
		t.Fatalf("unexpected guardian result: %+v", result)
	}
}
