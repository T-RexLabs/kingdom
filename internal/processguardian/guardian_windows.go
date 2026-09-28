//go:build windows

package processguardian

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func platformSupported() bool { return true }

func configureGuardianProcess(*exec.Cmd) error { return nil }

// Windows uses a suspended provider process assigned to a kill-on-close Job
// Object before it can execute. The Unix gate is unnecessary on this platform.
func runGuardian(specFile, control, status, stdout, stderr, stdin *os.File) error {
	defer closeFiles(specFile, control, status, stdout, stderr, stdin)
	var spec wireSpec
	if err := readFrame(specFile, &spec); err != nil {
		return err
	}
	if err := validateWireSpec(spec); err != nil {
		return err
	}
	if err := makeGuardianDescriptorsNonInheritable(specFile, control, status); err != nil {
		return err
	}
	jobName := "Local\\Kingdom-" + spec.GuardID
	jobNamePointer, err := windows.UTF16PtrFromString(jobName)
	if err != nil {
		return err
	}
	job, err := windows.CreateJobObject(nil, jobNamePointer)
	if err != nil {
		return fmt.Errorf("create provider Job Object: %w", err)
	}
	defer windows.CloseHandle(job)
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits))); err != nil {
		return fmt.Errorf("configure provider Job Object: %w", err)
	}
	process, thread, pid, err := createSuspendedProvider(spec, stdin, stdout, stderr)
	if err != nil {
		return err
	}
	_ = stdin.Close()
	defer windows.CloseHandle(process)
	defer windows.CloseHandle(thread)
	if err := windows.AssignProcessToJobObject(job, process); err != nil {
		_ = windows.TerminateProcess(process, 1)
		_, _ = windows.WaitForSingleObject(process, windows.INFINITE)
		return fmt.Errorf("assign provider to Job Object: %w", err)
	}
	guardianIdentity, err := windowsProcessIdentity(windows.CurrentProcess(), os.Getpid())
	if err != nil {
		return fmt.Errorf("identify guardian process: %w", err)
	}
	childIdentity, err := windowsProcessIdentity(process, pid)
	if err != nil {
		return fmt.Errorf("identify provider process: %w", err)
	}
	ownership := Ownership{GuardID: spec.GuardID, GuardianPID: os.Getpid(), GuardianIdentityToken: guardianIdentity, ChildPID: pid, ChildIdentityToken: childIdentity, TreeID: pid, TreeIdentityToken: jobName, Platform: "windows_job_object", QuiescenceCapability: spec.Capability}
	if err := writeFrame(status, wireStatus{Type: "ownership", Ownership: ownership}); err != nil {
		return err
	}
	command, err := readControl(control)
	if err != nil || (command != 'R' && command != 'D') {
		return publishWindowsResult(status, spec, ownership, job, process, true, "launch_aborted", "provider launch gate was not released")
	}
	spec.Durable = command == 'D'
	if _, err := windows.ResumeThread(thread); err != nil {
		return publishWindowsResult(status, spec, ownership, job, process, true, "release_failed", err.Error())
	}
	if err := writeFrame(status, wireStatus{Type: "started"}); err != nil {
		_ = windows.TerminateJobObject(job, 1)
		_, _ = windows.WaitForSingleObject(process, windows.INFINITE)
		return err
	}
	finished := make(chan struct{})
	go func() { _, _ = windows.WaitForSingleObject(process, windows.INFINITE); close(finished) }()
	select {
	case <-finished:
		if active, _ := windowsJobActive(job); active != 0 {
			return publishWindowsResult(status, spec, ownership, job, process, true, "provider_descendant_leak", "provider leader exited while Job Object remained active")
		}
		return publishWindowsResult(status, spec, ownership, job, process, false, "", "")
	case <-controlEOF(control):
		return publishWindowsResult(status, spec, ownership, job, process, true, "provider_cancelled", "guardian owner requested cancellation")
	}
}

func runGate(io.Reader, *os.File, *os.File) error {
	return fmt.Errorf("windows guardian does not use a gate process")
}
func terminateAcknowledgedTree(ownership Ownership, _, force time.Duration) error {
	name, err := windows.UTF16PtrFromString(ownership.TreeIdentityToken)
	if err != nil {
		return err
	}
	job, jobErr := openWindowsJobObject(name)
	if jobErr == nil {
		defer windows.CloseHandle(job)
		deadline := time.Now().Add(force)
		for time.Now().Before(deadline) {
			active, queryErr := windowsJobActive(job)
			if queryErr != nil {
				return queryErr
			}
			if active == 0 {
				return nil
			}
			time.Sleep(20 * time.Millisecond)
		}
		return fmt.Errorf("provider Job Object did not become empty after guardian exit")
	}
	if !windowsJobObjectNotFound(jobErr) {
		return fmt.Errorf("inspect provider Job Object after guardian exit: %w", jobErr)
	}
	process, err := windows.OpenProcess(windows.SYNCHRONIZE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(ownership.ChildPID))
	if err != nil {
		return windowsProcessOpenError(err)
	}
	defer windows.CloseHandle(process)
	wait := uint32(force.Milliseconds())
	if wait == 0 {
		wait = 1
	}
	result, err := windows.WaitForSingleObject(process, wait)
	if err != nil {
		return err
	}
	if result == windows.WAIT_OBJECT_0 {
		return nil
	}
	if result != uint32(windows.WAIT_TIMEOUT) {
		return fmt.Errorf("unexpected provider process wait result %d after guardian exit", result)
	}
	current, err := windowsProcessIdentity(process, ownership.ChildPID)
	if err != nil {
		return fmt.Errorf("inspect provider process identity after guardian exit: %w", err)
	}
	if current != ownership.ChildIdentityToken {
		return nil
	}
	return fmt.Errorf("provider leader remains active after guardian exit and Job Object inspection failed")
}

func windowsProcessOpenError(err error) error {
	if windowsProcessNotFound(err) {
		return nil
	}
	return fmt.Errorf("inspect provider process after guardian exit: %w", err)
}

func ManagedProcessAbsent(pid, _ int, identityToken, treeIdentityToken string) bool {
	if name, err := windows.UTF16PtrFromString(treeIdentityToken); err == nil {
		if job, err := openWindowsJobObject(name); err == nil {
			defer windows.CloseHandle(job)
			active, queryErr := windowsJobActive(job)
			return queryErr == nil && active == 0
		} else if !windowsJobObjectNotFound(err) {
			return false
		}
	} else {
		return false
	}
	process, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return windowsProcessNotFound(err)
	}
	defer windows.CloseHandle(process)
	wait, err := windows.WaitForSingleObject(process, 0)
	if err != nil {
		return false
	}
	if wait == windows.WAIT_OBJECT_0 {
		return true
	}
	if wait != uint32(windows.WAIT_TIMEOUT) {
		return false
	}
	current, err := windowsProcessIdentity(process, pid)
	return err == nil && current != identityToken
}

func windowsJobObjectNotFound(err error) bool {
	return errors.Is(err, windows.ERROR_FILE_NOT_FOUND) || errors.Is(err, windows.ERROR_INVALID_NAME)
}

func windowsProcessNotFound(err error) bool {
	return errors.Is(err, windows.ERROR_INVALID_PARAMETER)
}

var openJobObjectW = windows.NewLazySystemDLL("kernel32.dll").NewProc("OpenJobObjectW")

func openWindowsJobObject(name *uint16) (windows.Handle, error) {
	const jobObjectQuery = 0x0004
	handle, _, callErr := openJobObjectW.Call(jobObjectQuery, 0, uintptr(unsafe.Pointer(name)))
	if handle == 0 {
		return 0, callErr
	}
	return windows.Handle(handle), nil
}

func createSuspendedProvider(spec wireSpec, stdin, stdout, stderr *os.File) (windows.Handle, windows.Handle, int, error) {
	app, err := windows.UTF16PtrFromString(spec.Command.Path)
	if err != nil {
		return 0, 0, 0, err
	}
	parts := append([]string{syscall.EscapeArg(spec.Command.Path)}, spec.Command.Args...)
	for index := range parts[1:] {
		parts[index+1] = syscall.EscapeArg(parts[index+1])
	}
	line, err := windows.UTF16PtrFromString(strings.Join(parts, " "))
	if err != nil {
		return 0, 0, 0, err
	}
	if len(windows.UTF16PtrToString(line)) > 32766 {
		return 0, 0, 0, fmt.Errorf("guarded command exceeds Windows command-line limit")
	}
	var dir *uint16
	if spec.Command.Dir != "" {
		dir, err = windows.UTF16PtrFromString(spec.Command.Dir)
		if err != nil {
			return 0, 0, 0, err
		}
	}
	startup := windows.StartupInfo{Cb: uint32(unsafe.Sizeof(windows.StartupInfo{})), Flags: windows.STARTF_USESTDHANDLES, StdInput: windows.Handle(stdin.Fd()), StdOutput: windows.Handle(stdout.Fd()), StdErr: windows.Handle(stderr.Fd())}
	info := windows.ProcessInformation{}
	environment, err := windowsEnvironmentBlock(spec.Command.Env)
	if err != nil {
		return 0, 0, 0, err
	}
	var environmentPointer *uint16
	if len(environment) > 0 {
		environmentPointer = &environment[0]
	}
	if err := windows.CreateProcess(app, line, nil, nil, true, windows.CREATE_SUSPENDED|windows.CREATE_NO_WINDOW|windows.CREATE_UNICODE_ENVIRONMENT, environmentPointer, dir, &startup, &info); err != nil {
		return 0, 0, 0, err
	}
	return info.Process, info.Thread, int(info.ProcessId), nil
}

func makeGuardianDescriptorsNonInheritable(files ...*os.File) error {
	for _, file := range files {
		if file == nil {
			return fmt.Errorf("guardian descriptor is required")
		}
		if err := windows.SetHandleInformation(windows.Handle(file.Fd()), windows.HANDLE_FLAG_INHERIT, 0); err != nil {
			return fmt.Errorf("prevent guardian descriptor inheritance: %w", err)
		}
	}
	return nil
}

func windowsEnvironmentBlock(environment []string) ([]uint16, error) {
	if len(environment) == 0 {
		return nil, nil
	}
	block := make([]uint16, 0)
	for _, entry := range normalizeWindowsEnvironment(environment) {
		encoded, err := windows.UTF16FromString(entry)
		if err != nil {
			return nil, fmt.Errorf("encode provider environment: %w", err)
		}
		block = append(block, encoded...)
	}
	return append(block, 0), nil
}

func windowsProcessIdentity(process windows.Handle, pid int) (string, error) {
	var created, exited, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(process, &created, &exited, &kernel, &user); err != nil {
		return "", err
	}
	return fmt.Sprintf("%d:%d:%d", pid, created.HighDateTime, created.LowDateTime), nil
}

func processIdentityToken(pid int) (string, error) {
	process, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return "", fmt.Errorf("open process identity: %w", err)
	}
	defer windows.CloseHandle(process)
	return windowsProcessIdentity(process, pid)
}

func processIdentityNotFound(err error) bool {
	return errors.Is(err, windows.ERROR_INVALID_PARAMETER)
}

func configureCommandIdentity(*Command) error   { return nil }
func configureGuardianIdentity(*exec.Cmd) error { return nil }

func controlEOF(control io.Reader) <-chan struct{} {
	done := make(chan struct{})
	go func() { _, _ = readControl(control); close(done) }()
	return done
}

type windowsJobAccounting struct {
	TotalUserTime, TotalKernelTime, ThisPeriodTotalUserTime, ThisPeriodTotalKernelTime int64
	TotalPageFaultCount, TotalProcesses, ActiveProcesses, TotalTerminatedProcesses     uint32
}

func windowsJobActive(job windows.Handle) (uint32, error) {
	var accounting windowsJobAccounting
	err := windows.QueryInformationJobObject(job, windows.JobObjectBasicAccountingInformation, uintptr(unsafe.Pointer(&accounting)), uint32(unsafe.Sizeof(accounting)), nil)
	return accounting.ActiveProcesses, err
}
func publishWindowsResult(status *os.File, spec wireSpec, ownership Ownership, job, process windows.Handle, forced bool, code, text string) error {
	if forced {
		_ = windows.TerminateJobObject(job, 1)
	}
	_, _ = windows.WaitForSingleObject(process, windows.INFINITE)
	var exitCode uint32
	if err := windows.GetExitCodeProcess(process, &exitCode); err != nil {
		return err
	}
	var (
		active uint32
		err    error
	)
	if forced {
		active, err = waitForProcessesQuiescent(time.Duration(spec.CancelForceMS)*time.Millisecond, func() (uint32, error) {
			return windowsJobActive(job)
		})
	} else {
		active, err = windowsJobActive(job)
	}
	if err != nil {
		return err
	}
	result := Result{Ownership: ownership, ExitCode: int(exitCode), Forced: forced, Quiescent: active == 0, FailureCode: code, FailureText: text}
	if !result.Quiescent {
		result.FailureCode = "process_tree_unproven"
		result.FailureText = "provider Job Object did not become empty"
	}
	if digest, evidence, err := publishEvidence(spec, result); err != nil {
		result.Quiescent = false
		result.FailureCode = "quiescence_evidence_failed"
		result.FailureText = err.Error()
	} else {
		result.EvidenceSHA256 = digest
		result.EvidenceJSON = evidence
	}
	return writeFrame(status, wireStatus{Type: "result", Result: result})
}
