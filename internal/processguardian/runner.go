package processguardian

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func NewRunner(cfg Config) (*Runner, error) {
	executable := strings.TrimSpace(cfg.Executable)
	if executable == "" {
		var err error
		executable, err = os.Executable()
		if err != nil {
			return nil, fmt.Errorf("resolve guardian executable: %w", err)
		}
	}
	if _, err := os.Stat(executable); err != nil {
		return nil, fmt.Errorf("stat guardian executable: %w", err)
	}
	if !platformSupported() {
		return nil, fmt.Errorf("process guardian is unsupported on this platform")
	}
	runtimeRoot := strings.TrimSpace(cfg.RuntimeRoot)
	if runtimeRoot != "" {
		if err := os.MkdirAll(runtimeRoot, 0o700); err != nil {
			return nil, fmt.Errorf("create process guardian runtime root: %w", err)
		}
		info, err := os.Lstat(runtimeRoot)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("process guardian runtime root must be a non-symlink directory")
		}
	}
	if cfg.CancelGrace <= 0 {
		cfg.CancelGrace = 5 * time.Second
	}
	if cfg.CancelForce <= 0 {
		cfg.CancelForce = 5 * time.Second
	}
	return &Runner{executable: executable, runtimeRoot: runtimeRoot, cancelGrace: cfg.CancelGrace, cancelForce: cfg.CancelForce}, nil
}

// Start does not return until the caller has acknowledged durable ownership and
// the guardian has released the provider exec gate.
func (r *Runner) Start(ctx context.Context, command Command, persist PersistOwnership) (Handle, error) {
	if r == nil {
		return nil, fmt.Errorf("process guardian runner is required")
	}
	if strings.TrimSpace(command.Path) == "" {
		return nil, fmt.Errorf("guardian command path is required")
	}
	resolvedPath, err := exec.LookPath(command.Path)
	if err != nil {
		return nil, fmt.Errorf("resolve guarded command %q: %w", command.Path, err)
	}
	command.Path, err = filepath.Abs(resolvedPath)
	if err != nil {
		return nil, fmt.Errorf("resolve absolute guarded command path: %w", err)
	}
	guardID, err := newRandomHex(16)
	if err != nil {
		return nil, fmt.Errorf("generate guard id: %w", err)
	}
	capability, err := newRandomHex(32)
	if err != nil {
		return nil, fmt.Errorf("generate quiescence capability: %w", err)
	}
	specReader, specWriter, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer specWriter.Close()
	controlReader, controlWriter, err := os.Pipe()
	if err != nil {
		specReader.Close()
		return nil, err
	}
	statusReader, statusWriter, err := os.Pipe()
	if err != nil {
		specReader.Close()
		controlReader.Close()
		controlWriter.Close()
		return nil, err
	}
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		specReader.Close()
		controlReader.Close()
		controlWriter.Close()
		statusReader.Close()
		statusWriter.Close()
		return nil, err
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		specReader.Close()
		controlReader.Close()
		controlWriter.Close()
		statusReader.Close()
		statusWriter.Close()
		stdoutReader.Close()
		stdoutWriter.Close()
		return nil, err
	}

	cmd := exec.Command(r.executable, HiddenGuardianMode)
	configureGuardianIO(cmd, specReader, controlReader, statusWriter, stdoutWriter, stderrWriter)
	if err := configureGuardianProcess(cmd); err != nil {
		closeFiles(specReader, controlReader, controlWriter, statusReader, statusWriter, stdoutReader, stdoutWriter, stderrReader, stderrWriter)
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		closeFiles(specReader, controlReader, controlWriter, statusReader, statusWriter, stdoutReader, stdoutWriter, stderrReader, stderrWriter)
		return nil, fmt.Errorf("start process guardian: %w", err)
	}
	closeFiles(specReader, controlReader, statusWriter, stdoutWriter, stderrWriter)

	spec := wireSpec{Version: "v1", GuardID: guardID, Capability: capability, RuntimeRoot: r.runtimeRoot, CancelGraceMS: r.cancelGrace.Milliseconds(), CancelForceMS: r.cancelForce.Milliseconds(), Command: command}
	if err := writeFrame(specWriter, spec); err != nil {
		closeFiles(specWriter, controlWriter, statusReader, stdoutReader, stderrReader)
		_ = cmd.Wait()
		return nil, fmt.Errorf("send guardian launch specification: %w", err)
	}
	if err := specWriter.Close(); err != nil {
		closeFiles(controlWriter, statusReader, stdoutReader, stderrReader)
		_ = cmd.Wait()
		return nil, fmt.Errorf("close guardian launch specification: %w", err)
	}

	var status wireStatus
	if err := readFrame(statusReader, &status); err != nil {
		closeFiles(controlWriter, statusReader, stdoutReader, stderrReader)
		_ = cmd.Wait()
		return nil, fmt.Errorf("read guardian ownership acknowledgement: %w", err)
	}
	if status.Type != "ownership" || status.Ownership.GuardID != guardID || status.Ownership.ChildPID <= 0 || status.Ownership.TreeID <= 0 {
		closeFiles(controlWriter, statusReader, stdoutReader, stderrReader)
		_ = cmd.Wait()
		return nil, fmt.Errorf("invalid guardian ownership acknowledgement")
	}
	status.Ownership.GuardianPID = cmd.Process.Pid
	status.Ownership.QuiescenceCapability = capability
	if persist != nil {
		if err := persist(ctx, status.Ownership); err != nil {
			_ = controlWriter.Close()
			result, resultErr := readLaunchAbortResult(statusReader, cmd, status.Ownership)
			closeFiles(stdoutReader, stderrReader)
			if resultErr != nil {
				return nil, fmt.Errorf("persist guardian ownership: %w", errors.Join(err, resultErr))
			}
			return nil, &LaunchError{Cause: fmt.Errorf("persist guardian ownership: %w", err), Result: result}
		}
	}
	releaseCommand := byte('R')
	if persist != nil {
		releaseCommand = 'D'
	}
	if _, err := controlWriter.Write([]byte{releaseCommand}); err != nil {
		closeFiles(controlWriter, statusReader, stdoutReader, stderrReader)
		_ = cmd.Wait()
		return nil, fmt.Errorf("release provider launch gate: %w", err)
	}
	var started wireStatus
	if err := readFrame(statusReader, &started); err != nil || started.Type != "started" {
		_ = controlWriter.Close()
		cleanupErr := terminateAcknowledgedTree(status.Ownership, r.cancelGrace, r.cancelForce)
		result, resultErr := readLaunchAbortResultAfterStatus(started, statusReader, cmd, status.Ownership)
		closeFiles(stdoutReader, stderrReader)
		return nil, &LaunchError{Cause: errors.Join(fmt.Errorf("provider exec acknowledgement failed: %w", err), cleanupErr, resultErr), Result: result}
	}
	handle := &handle{guardID: guardID, ownership: status.Ownership, control: controlWriter, status: statusReader, stdout: stdoutReader, stderr: stderrReader, guardian: cmd, runtimeRoot: r.runtimeRoot, cancelGrace: r.cancelGrace, cancelForce: r.cancelForce, done: make(chan struct{})}
	handle.startWait()
	return handle, nil
}

func readLaunchAbortResultAfterStatus(first wireStatus, status *os.File, guardian *exec.Cmd, ownership Ownership) (Result, error) {
	if first.Type == "result" {
		_ = guardian.Wait()
		return first.Result, nil
	}
	return readLaunchAbortResult(status, guardian, ownership)
}

func readLaunchAbortResult(status *os.File, guardian *exec.Cmd, ownership Ownership) (Result, error) {
	defer status.Close()
	var terminal wireStatus
	if err := readFrame(status, &terminal); err != nil {
		_ = guardian.Wait()
		return Result{}, fmt.Errorf("read aborted guardian result: %w", err)
	}
	if err := guardian.Wait(); err != nil && terminal.Result.FailureText == "" {
		return Result{}, fmt.Errorf("wait for aborted guardian: %w", err)
	}
	if terminal.Type != "result" || terminal.Result.GuardID != ownership.GuardID || !terminal.Result.Quiescent {
		return Result{}, fmt.Errorf("aborted guardian did not prove quiescence")
	}
	terminal.Result.GuardianPID = ownership.GuardianPID
	terminal.Result.GuardianIdentityToken = ownership.GuardianIdentityToken
	terminal.Result.QuiescenceCapability = ownership.QuiescenceCapability
	return terminal.Result, nil
}

type handleResult struct {
	result Result
	err    error
}

type handle struct {
	guardID     string
	ownership   Ownership
	control     *os.File
	status      *os.File
	stdout      *os.File
	stderr      *os.File
	guardian    *exec.Cmd
	runtimeRoot string
	result      handleResult
	done        chan struct{}
	cancelGrace time.Duration
	cancelForce time.Duration
	waitOnce    sync.Once
	cancelOnce  sync.Once
}

func (h *handle) GuardID() string       { return h.guardID }
func (h *handle) Stdout() io.ReadCloser { return h.stdout }
func (h *handle) Stderr() io.ReadCloser { return h.stderr }

func (h *handle) Cancel(_ context.Context) error {
	if h == nil {
		return nil
	}
	var result error
	h.cancelOnce.Do(func() { _, result = h.control.Write([]byte{'C'}); _ = h.control.Close() })
	return result
}

func (h *handle) Wait(ctx context.Context) (Result, error) {
	if h == nil {
		return Result{}, fmt.Errorf("guardian handle is required")
	}
	select {
	case <-h.done:
		return h.result.result, h.result.err
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

func (h *handle) startWait() {
	h.waitOnce.Do(func() {
		go func() {
			defer close(h.done)
			defer h.status.Close()
			defer h.control.Close()
			var terminal wireStatus
			if err := readFrame(h.status, &terminal); err != nil {
				defer h.guardian.Wait()
				cleanupErr := terminateAcknowledgedTree(h.ownership, h.cancelGrace, h.cancelForce)
				result := Result{Ownership: h.ownership, Forced: true, Quiescent: cleanupErr == nil, FailureCode: "guardian_failed", FailureText: "provider guardian exited before publishing quiescence"}
				if cleanupErr == nil {
					result.EvidenceSHA256, result.EvidenceJSON, cleanupErr = publishEvidence(wireSpec{GuardID: h.guardID, RuntimeRoot: h.runtimeRoot, Durable: true}, result)
				}
				h.result.result = result
				h.result.err = errors.Join(fmt.Errorf("read guardian result: %w", err), cleanupErr)
				return
			}
			if terminal.Type != "result" || terminal.Result.GuardID != h.guardID || !terminal.Result.Quiescent {
				h.result.err = errors.Join(fmt.Errorf("guardian returned unproven quiescence: %s", terminal.Error), terminateAcknowledgedTree(h.ownership, h.cancelGrace, h.cancelForce))
				return
			}
			if err := h.guardian.Wait(); err != nil && terminal.Result.FailureText == "" {
				h.result.err = fmt.Errorf("wait for guardian: %w", err)
				return
			}
			terminal.Result.GuardianPID = h.ownership.GuardianPID
			terminal.Result.GuardianIdentityToken = h.ownership.GuardianIdentityToken
			terminal.Result.QuiescenceCapability = h.ownership.QuiescenceCapability
			h.result.result = terminal.Result
		}()
	})
}

func closeFiles(files ...*os.File) {
	for _, file := range files {
		if file != nil {
			_ = file.Close()
		}
	}
}
