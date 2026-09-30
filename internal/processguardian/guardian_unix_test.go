//go:build darwin || linux

package processguardian

import (
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestManagedProcessAbsentTreatsReusedProcessGroupIDAsAbsent(t *testing.T) {
	provider := exec.Command("sleep", "60")
	provider.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := provider.Start(); err != nil {
		t.Fatalf("start provider process group: %v", err)
	}
	t.Cleanup(func() {
		_ = provider.Process.Kill()
		_ = provider.Wait()
	})

	pid := provider.Process.Pid
	identity, err := processIdentityToken(pid)
	if err != nil {
		t.Fatalf("identify provider process: %v", err)
	}
	if ManagedProcessAbsent(pid, pid, identity, identity) {
		t.Fatal("matching live process group was reported absent")
	}
	if groupAbsent(pid) {
		t.Fatalf("current process group %d unexpectedly absent", pid)
	}
	if !ManagedProcessAbsent(pid, pid, "previous-process-generation", "previous-process-generation") {
		t.Fatal("live process group with a different leader generation was treated as the recorded provider tree")
	}
}

func TestTerminateAcknowledgedTreeDoesNotSignalReusedProcessGroupID(t *testing.T) {
	provider := exec.Command("sleep", "60")
	provider.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := provider.Start(); err != nil {
		t.Fatalf("start provider process group: %v", err)
	}
	t.Cleanup(func() {
		_ = provider.Process.Kill()
		_ = provider.Wait()
	})
	pid := provider.Process.Pid
	if groupAbsent(pid) {
		t.Fatalf("current process group %d unexpectedly absent", pid)
	}
	ownership := Ownership{
		ChildPID:           pid,
		ChildIdentityToken: "previous-process-generation",
		TreeID:             pid,
		TreeIdentityToken:  "previous-process-generation",
		Platform:           "unix_process_group",
	}
	if err := terminateAcknowledgedTree(ownership, 10*time.Millisecond, 10*time.Millisecond); err != nil {
		t.Fatalf("terminate stale process-group generation: %v", err)
	}
	if groupAbsent(pid) {
		t.Fatal("stale guard terminated the replacement process group")
	}
}
