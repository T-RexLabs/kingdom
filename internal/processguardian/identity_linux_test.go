//go:build linux

package processguardian

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestLinuxProcessIdentityDistinguishesSameTickGenerations(t *testing.T) {
	root := newLinuxProcFixture(t)
	writeLinuxProcFixture(t, root, 123, 123, "S", 1, 42, "generation-one")
	first, err := linuxProcessIdentitySnapshotAt(root, 123)
	if err != nil {
		t.Fatal(err)
	}
	writeLinuxProcFixture(t, root, 123, 123, "S", 1, 42, "generation-two")
	second, err := linuxProcessIdentitySnapshotAt(root, 123)
	if err != nil {
		t.Fatal(err)
	}
	if first.identity == second.identity {
		t.Fatalf("same-tick process generations share identity %q", first.identity)
	}
}

func TestLinuxAcknowledgedZombieLeaderRetainsLiveDescendantFence(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "descendant.pid")
	command := Command{Env: os.Environ()}
	if err := configureCommandIdentity(&command); err != nil {
		t.Fatal(err)
	}
	leader := exec.Command("/bin/sh", "-c", "read release; sleep 60 & echo $! > "+shellQuote(marker)+"; exit 0")
	leader.Env = command.Env
	leader.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	release, err := leader.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := leader.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		release.Close()
		_ = syscall.Kill(-leader.Process.Pid, syscall.SIGKILL)
		_ = leader.Wait()
	})
	identity, err := processIdentityToken(leader.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(identity, ":instance:") {
		t.Fatalf("leader lacks the configured process generation marker: %q", identity)
	}
	if _, err := release.Write([]byte("release\n")); err != nil {
		t.Fatal(err)
	}
	if err := waitForFile(marker, time.Second); err != nil {
		t.Fatal(err)
	}
	waitForLinuxZombie(t, leader.Process.Pid)
	current, currentErr := processIdentityToken(leader.Process.Pid)
	t.Logf("unreaped leader identity: expected=%q current=%q err=%v", identity, current, currentErr)
	ownership := Ownership{ChildPID: leader.Process.Pid, TreeID: leader.Process.Pid, ChildIdentityToken: identity, TreeIdentityToken: identity}
	if absent, err := acknowledgedTreeAbsent(ownership); err != nil || absent {
		t.Fatalf("zombie leader lost its live descendant fence: absent=%t err=%v", absent, err)
	}
}

func TestLinuxZombieOnlyProcessGroupIsQuiescent(t *testing.T) {
	leader := exec.CommandContext(context.Background(), "/bin/sh", "-c", "exit 0")
	leader.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := leader.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = leader.Wait() })
	waitForLinuxZombie(t, leader.Process.Pid)
	if !groupAbsent(leader.Process.Pid) {
		t.Fatal("a terminated zombie-only process group was treated as active")
	}
}

func TestLinuxProcessIdentityRecognizesSearchError(t *testing.T) {
	if !processIdentityNotFound(fmt.Errorf("read process environment: %w", syscall.ESRCH)) {
		t.Fatal("an exited process search error was not recognized")
	}
}

func waitForLinuxZombie(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if err != nil {
			t.Fatal(err)
		}
		closing := strings.LastIndex(string(raw), ")")
		if closing >= 0 && strings.HasPrefix(strings.TrimSpace(string(raw[closing+1:])), "Z ") {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("leader did not reach its unreaped zombie state")
}

func TestConfigureCommandIdentityReplacesCallerToken(t *testing.T) {
	command := Command{Env: []string{"PATH=/usr/bin", processIdentityEnvironmentName + "=caller-value"}}
	if err := configureCommandIdentity(&command); err != nil {
		t.Fatalf("configure command identity: %v", err)
	}
	if token := processIdentityEnvironmentValue(command.Env); len(token) != 32 || token == "caller-value" {
		t.Fatalf("configured process identity token=%q, want a generated 32-character token", token)
	}
	if len(command.Env) != 2 || command.Env[0] != "PATH=/usr/bin" {
		t.Fatalf("command environment after identity setup=%q", command.Env)
	}
}

func TestLinuxCurrentProcessIdentityIncludesStableFileMarker(t *testing.T) {
	first, err := ProcessIdentityToken(os.Getpid())
	if err != nil {
		t.Fatalf("read current process identity: %v", err)
	}
	second, err := ProcessIdentityToken(os.Getpid())
	if err != nil {
		t.Fatalf("read current process identity again: %v", err)
	}
	if first != second || !strings.Contains(first, ":fd:") {
		t.Fatalf("current process identity changed or lacks file marker: first=%q second=%q", first, second)
	}
}
