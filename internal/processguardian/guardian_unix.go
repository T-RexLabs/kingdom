//go:build darwin || linux

package processguardian

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

func platformSupported() bool { return true }

func configureGuardianProcess(cmd *exec.Cmd) error {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return nil
}

func runGuardian(specFile, control, status, stdout, stderr *os.File) error {
	defer closeFiles(specFile, control, status, stdout, stderr)
	var spec wireSpec
	if err := readFrame(specFile, &spec); err != nil {
		return fmt.Errorf("read launch specification: %w", err)
	}
	if err := validateWireSpec(spec); err != nil {
		return err
	}

	releaseRead, releaseWrite, err := os.Pipe()
	if err != nil {
		return err
	}
	execStatusRead, execStatusWrite, err := os.Pipe()
	if err != nil {
		closeFiles(releaseRead, releaseWrite)
		return err
	}
	gate := exec.Command(os.Args[0], HiddenGateMode)
	gate.Stdin = newWireSpecReader(spec)
	gate.Stdout = stdout
	gate.Stderr = stderr
	gate.ExtraFiles = []*os.File{releaseRead, execStatusWrite}
	gate.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := gate.Start(); err != nil {
		closeFiles(releaseRead, releaseWrite, execStatusRead, execStatusWrite)
		return fmt.Errorf("start gated provider: %w", err)
	}
	_ = releaseRead.Close()
	_ = execStatusWrite.Close()
	guardianIdentity, err := processIdentityToken(os.Getpid())
	if err != nil {
		_ = releaseWrite.Close()
		_ = gate.Wait()
		return fmt.Errorf("identify guardian process: %w", err)
	}
	childIdentity, err := processIdentityToken(gate.Process.Pid)
	if err != nil {
		_ = releaseWrite.Close()
		_ = gate.Wait()
		return fmt.Errorf("identify gated provider process: %w", err)
	}
	ownership := Ownership{GuardID: spec.GuardID, GuardianPID: os.Getpid(), GuardianIdentityToken: guardianIdentity, ChildPID: gate.Process.Pid, ChildIdentityToken: childIdentity, TreeID: gate.Process.Pid, TreeIdentityToken: childIdentity, Platform: "unix_process_group", QuiescenceCapability: spec.Capability}
	if err := writeFrame(status, wireStatus{Type: "ownership", Ownership: ownership}); err != nil {
		_ = terminateGroup(ownership.TreeID, spec.CancelGraceMS, spec.CancelForceMS)
		_ = gate.Wait()
		return fmt.Errorf("write ownership acknowledgement: %w", err)
	}

	command, readErr := readControl(control)
	if readErr != nil || (command != 'R' && command != 'D') {
		_ = releaseWrite.Close()
		result := waitAndQuiesce(gate, ownership, spec, true, "launch_aborted", "provider launch gate was not released")
		result.EvidenceSHA256, result.EvidenceJSON, _ = publishEvidence(spec, result)
		_ = writeFrame(status, wireStatus{Type: "result", Result: result})
		return nil
	}
	spec.Durable = command == 'D'
	if _, err := releaseWrite.Write([]byte{'R'}); err != nil {
		_ = releaseWrite.Close()
		result := waitAndQuiesce(gate, ownership, spec, true, "release_failed", err.Error())
		result.EvidenceSHA256, result.EvidenceJSON, _ = publishEvidence(spec, result)
		_ = writeFrame(status, wireStatus{Type: "result", Result: result})
		return nil
	}
	_ = releaseWrite.Close()
	var execStatus [1]byte
	if _, err := execStatusRead.Read(execStatus[:]); err != io.EOF {
		_ = execStatusRead.Close()
		result := waitAndQuiesce(gate, ownership, spec, true, "provider_exec_failed", "provider command failed before exec")
		result.EvidenceSHA256, result.EvidenceJSON, _ = publishEvidence(spec, result)
		_ = writeFrame(status, wireStatus{Type: "result", Result: result})
		return nil
	}
	_ = execStatusRead.Close()
	if err := writeFrame(status, wireStatus{Type: "started"}); err != nil {
		_ = terminateGroup(ownership.TreeID, spec.CancelGraceMS, spec.CancelForceMS)
		_ = gate.Wait()
		return err
	}

	waitDone := make(chan error, 1)
	go func() { waitDone <- gate.Wait() }()
	controlDone := make(chan byte, 1)
	go func() { value, _ := readControl(control); controlDone <- value }()
	var waitErr error
	forced := false
	failureCode := ""
	failureText := ""
	select {
	case waitErr = <-waitDone:
		if !groupAbsent(ownership.TreeID) {
			forced = true
			failureCode = "provider_descendant_leak"
			failureText = "provider leader exited while its process group remained alive"
			_ = terminateGroup(ownership.TreeID, spec.CancelGraceMS, spec.CancelForceMS)
		}
	case <-controlDone:
		forced = true
		failureCode = "provider_cancelled"
		failureText = "guardian owner requested cancellation"
		_ = terminateGroup(ownership.TreeID, spec.CancelGraceMS, spec.CancelForceMS)
		waitErr = <-waitDone
	}
	result := resultFromWait(ownership, waitErr, forced, failureCode, failureText)
	result.Quiescent = groupAbsent(ownership.TreeID)
	if !result.Quiescent {
		result.FailureCode = "process_tree_unproven"
		result.FailureText = "provider process group did not become absent"
	}
	if evidenceSHA256, evidenceJSON, err := publishEvidence(spec, result); err != nil {
		result.Quiescent = false
		result.FailureCode = "quiescence_evidence_failed"
		result.FailureText = err.Error()
	} else {
		result.EvidenceSHA256 = evidenceSHA256
		result.EvidenceJSON = evidenceJSON
	}
	if err := writeFrame(status, wireStatus{Type: "result", Result: result}); err != nil {
		return err
	}
	return nil
}

func runGate(specReader io.Reader, release *os.File) error {
	defer release.Close()
	var spec wireSpec
	if err := readFrame(specReader, &spec); err != nil {
		return err
	}
	if err := validateWireSpec(spec); err != nil {
		return err
	}
	var command [1]byte
	if _, err := io.ReadFull(release, command[:]); err != nil {
		return fmt.Errorf("provider launch gate closed: %w", err)
	}
	if command[0] != 'R' {
		return fmt.Errorf("invalid provider launch gate command")
	}
	execStatus := os.NewFile(4, "provider-exec-status")
	defer execStatus.Close()
	// The gate retains only stdin/stdout/stderr and its release pipe. Inherited
	// guardian control or status descriptors would keep EOF from reaching the
	// parent when the guardian crashes, leaving the provider tree unowned.
	for fd := 5; fd < 32; fd++ {
		_ = syscall.Close(fd)
	}
	argv := append([]string{spec.Command.Path}, spec.Command.Args...)
	if err := os.Chdir(spec.Command.Dir); err != nil && spec.Command.Dir != "" {
		_, _ = execStatus.Write([]byte{'F'})
		return err
	}
	syscall.CloseOnExec(4)
	if err := syscall.Exec(spec.Command.Path, argv, spec.Command.Env); err != nil {
		_, _ = execStatus.Write([]byte{'F'})
		return err
	}
	return nil
}

func groupAbsent(pgid int) bool {
	err := syscall.Kill(-pgid, syscall.Signal(0))
	return errors.Is(err, syscall.ESRCH)
}

func ManagedProcessAbsent(pid, treeID int, identityToken, _ string) bool {
	if current, err := processIdentityToken(pid); err == nil && current == identityToken {
		return false
	}
	return groupAbsent(treeID)
}

func terminateGroup(pgid int, graceMS, forceMS int64) error {
	if pgid <= 0 || groupAbsent(pgid) {
		return nil
	}
	if err := syscall.Kill(-pgid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	if waitGroupAbsent(pgid, time.Duration(graceMS)*time.Millisecond) {
		return nil
	}
	if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	if !waitGroupAbsent(pgid, time.Duration(forceMS)*time.Millisecond) {
		return fmt.Errorf("process group %d did not exit", pgid)
	}
	return nil
}

func terminateAcknowledgedTree(ownership Ownership, grace, force time.Duration) error {
	return terminateGroup(ownership.TreeID, grace.Milliseconds(), force.Milliseconds())
}

func waitGroupAbsent(pgid int, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if groupAbsent(pgid) {
			return true
		}
		select {
		case <-deadline.C:
			return groupAbsent(pgid)
		case <-ticker.C:
		}
	}
}

func resultFromWait(ownership Ownership, waitErr error, forced bool, code, text string) Result {
	result := Result{Ownership: ownership, Forced: forced, FailureCode: code, FailureText: text}
	if waitErr == nil {
		return result
	}
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			result.ExitCode = status.ExitStatus()
			if status.Signaled() {
				result.ExitSignal = int(status.Signal())
			}
		}
		return result
	}
	if result.FailureText == "" {
		result.FailureText = waitErr.Error()
	}
	if result.FailureCode == "" {
		result.FailureCode = "provider_wait_failed"
	}
	return result
}

func waitAndQuiesce(gate *exec.Cmd, ownership Ownership, spec wireSpec, forced bool, code, text string) Result {
	// An unreleased gate exits on release-pipe EOF. Reap it before probing its
	// process group, otherwise a zombie leader can falsely keep the group alive.
	waitErr := gate.Wait()
	_ = terminateGroup(ownership.TreeID, spec.CancelGraceMS, spec.CancelForceMS)
	returnResult := resultFromWait(ownership, waitErr, forced, code, text)
	returnResult.Quiescent = groupAbsent(ownership.TreeID)
	return returnResult
}

func newWireSpecReader(spec wireSpec) io.Reader {
	var buffer bytes.Buffer
	_ = writeFrame(&buffer, spec)
	return &buffer
}
