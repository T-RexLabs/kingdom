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
	if err := ctx.Err(); err != nil {
		return nil, err
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
	statuses := startStatusReader(statusReader)
	abortUnacknowledged := func() {
		closeFiles(specWriter, controlWriter, stdoutReader, stderrReader)
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		statuses.shutdown()
	}

	spec := wireSpec{Version: "v1", GuardID: guardID, Capability: capability, RuntimeRoot: r.runtimeRoot, CancelGraceMS: r.cancelGrace.Milliseconds(), CancelForceMS: r.cancelForce.Milliseconds(), Command: command}
	if err := writeFrameContext(ctx, specWriter, spec); err != nil {
		abortUnacknowledged()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("send guardian launch specification: %w", err)
	}
	if err := specWriter.Close(); err != nil {
		abortUnacknowledged()
		return nil, fmt.Errorf("close guardian launch specification: %w", err)
	}

	status, err := nextStatus(ctx, statuses.updates)
	if err != nil {
		abortUnacknowledged()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("read guardian ownership acknowledgement: %w", err)
	}
	if status.Type != "ownership" || status.Ownership.GuardID != guardID || status.Ownership.ChildPID <= 0 || status.Ownership.TreeID <= 0 {
		abortUnacknowledged()
		return nil, fmt.Errorf("invalid guardian ownership acknowledgement")
	}
	status.Ownership.GuardianPID = cmd.Process.Pid
	status.Ownership.QuiescenceCapability = capability
	ownershipPersisted := false
	if persist != nil {
		if err := persistOwnership(ctx, persist, status.Ownership); err != nil {
			result, resultErr := r.abortAcknowledgedLaunch(controlWriter, statuses, cmd, status.Ownership, false)
			closeFiles(stdoutReader, stderrReader)
			return nil, &LaunchError{Cause: errors.Join(fmt.Errorf("persist guardian ownership: %w", err), resultErr), Result: result}
		}
		ownershipPersisted = true
	}
	if err := ctx.Err(); err != nil {
		result, resultErr := r.abortAcknowledgedLaunch(controlWriter, statuses, cmd, status.Ownership, ownershipPersisted)
		closeFiles(stdoutReader, stderrReader)
		return nil, &LaunchError{Cause: errors.Join(err, resultErr), Result: result}
	}
	releaseCommand := byte('R')
	if ownershipPersisted {
		releaseCommand = 'D'
	}
	if err := writeControlContext(ctx, controlWriter, releaseCommand); err != nil {
		result, resultErr := r.abortAcknowledgedLaunch(controlWriter, statuses, cmd, status.Ownership, ownershipPersisted)
		closeFiles(stdoutReader, stderrReader)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, &LaunchError{Cause: errors.Join(ctxErr, resultErr), Result: result}
		}
		return nil, &LaunchError{Cause: errors.Join(fmt.Errorf("release provider launch gate: %w", err), resultErr), Result: result}
	}
	if err := ctx.Err(); err != nil {
		result, resultErr := r.abortAcknowledgedLaunch(controlWriter, statuses, cmd, status.Ownership, ownershipPersisted)
		closeFiles(stdoutReader, stderrReader)
		return nil, &LaunchError{Cause: errors.Join(err, resultErr), Result: result}
	}
	started, err := nextStatus(ctx, statuses.updates)
	if err != nil || started.Type != "started" {
		cause := fmt.Errorf("provider exec acknowledgement failed")
		if err != nil {
			cause = fmt.Errorf("provider exec acknowledgement failed: %w", err)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			cause = ctxErr
		}
		result, resultErr := r.abortAcknowledgedLaunch(controlWriter, statuses, cmd, status.Ownership, ownershipPersisted)
		closeFiles(stdoutReader, stderrReader)
		return nil, &LaunchError{Cause: errors.Join(cause, resultErr), Result: result}
	}
	if err := ctx.Err(); err != nil {
		result, resultErr := r.abortAcknowledgedLaunch(controlWriter, statuses, cmd, status.Ownership, ownershipPersisted)
		closeFiles(stdoutReader, stderrReader)
		return nil, &LaunchError{Cause: errors.Join(err, resultErr), Result: result}
	}
	handle := &handle{guardID: guardID, ownership: status.Ownership, control: controlWriter, statuses: statuses, stdout: stdoutReader, stderr: stderrReader, guardian: cmd, runtimeRoot: r.runtimeRoot, cancelGrace: r.cancelGrace, cancelForce: r.cancelForce, done: make(chan struct{})}
	handle.startWait()
	return handle, nil
}

type statusRead struct {
	status wireStatus
	err    error
}

type statusReaderHandle struct {
	updates  chan statusRead
	done     chan struct{}
	cancel   context.CancelFunc
	status   *os.File
	stopOnce sync.Once
}

func startStatusReader(status *os.File) *statusReaderHandle {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &statusReaderHandle{updates: make(chan statusRead, 1), done: make(chan struct{}), cancel: cancel, status: status}
	go func() {
		defer close(reader.done)
		defer close(reader.updates)
		defer status.Close()
		for {
			var value wireStatus
			err := readFrame(status, &value)
			select {
			case reader.updates <- statusRead{status: value, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return reader
}

func (r *statusReaderHandle) shutdown() {
	if r == nil {
		return
	}
	r.stopOnce.Do(func() {
		r.cancel()
		_ = r.status.Close()
	})
	<-r.done
}

func nextStatus(ctx context.Context, statuses <-chan statusRead) (wireStatus, error) {
	if err := ctx.Err(); err != nil {
		return wireStatus{}, err
	}
	select {
	case received, ok := <-statuses:
		if err := ctx.Err(); err != nil {
			return wireStatus{}, err
		}
		if !ok {
			return wireStatus{}, io.EOF
		}
		return received.status, received.err
	case <-ctx.Done():
		return wireStatus{}, ctx.Err()
	}
}

func writeFrameContext(ctx context.Context, file *os.File, value any) error {
	done := make(chan error, 1)
	go func() { done <- writeFrame(file, value) }()
	select {
	case err := <-done:
		if err == nil {
			return ctx.Err()
		}
		return err
	case <-ctx.Done():
		_ = file.Close()
		<-done
		return ctx.Err()
	}
}

func writeControlContext(ctx context.Context, file *os.File, command byte) error {
	done := make(chan error, 1)
	go func() {
		_, err := file.Write([]byte{command})
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		_ = file.Close()
		<-done
		return ctx.Err()
	}
}

func persistOwnership(ctx context.Context, persist PersistOwnership, ownership Ownership) error {
	done := make(chan error, 1)
	go func() { done <- persist(ctx, ownership) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// Once persistence has begun, its outcome decides whether abort must
		// produce durable quiescence evidence. Do not race cancellation against
		// a callback that may have just committed ownership.
		return <-done
	}
}

func (r *Runner) abortAcknowledgedLaunch(control *os.File, statuses *statusReaderHandle, guardian *exec.Cmd, ownership Ownership, durable bool) (Result, error) {
	_ = control.Close()
	defer statuses.shutdown()
	cleanupDone := make(chan error, 1)
	go func() { cleanupDone <- terminateAcknowledgedTree(ownership, r.cancelGrace, r.cancelForce) }()
	guardianDone := make(chan error, 1)
	go func() { guardianDone <- guardian.Wait() }()

	deadline := time.NewTimer(r.cancelGrace + r.cancelForce)
	defer deadline.Stop()
	var (
		result       Result
		resultErr    error
		cleanupErr   error
		guardianErr  error
		haveResult   bool
		haveCleanup  bool
		haveGuardian bool
	)
	updates := statuses.updates
	for !haveResult || !haveCleanup || !haveGuardian {
		select {
		case received, ok := <-updates:
			if !ok {
				haveResult = true
				resultErr = fmt.Errorf("read aborted guardian result: %w", io.EOF)
				updates = nil
				continue
			}
			if received.err != nil {
				haveResult = true
				resultErr = fmt.Errorf("read aborted guardian result: %w", received.err)
				updates = nil
				continue
			}
			if received.status.Type != "result" {
				continue
			}
			haveResult = true
			updates = nil
			result = received.status.Result
			if result.GuardID != ownership.GuardID || !result.Quiescent || (durable && !r.hasDurableQuiescenceEvidence(ownership, result)) {
				resultErr = fmt.Errorf("aborted guardian did not prove quiescence")
			} else {
				result.GuardianPID = ownership.GuardianPID
				result.GuardianIdentityToken = ownership.GuardianIdentityToken
				result.QuiescenceCapability = ownership.QuiescenceCapability
			}
		case cleanupErr = <-cleanupDone:
			haveCleanup = true
		case guardianErr = <-guardianDone:
			haveGuardian = true
		case <-deadline.C:
			_ = guardian.Process.Kill()
			if !haveCleanup {
				cleanupErr = <-cleanupDone
				haveCleanup = true
			}
			if !haveGuardian {
				guardianErr = <-guardianDone
				haveGuardian = true
			}
			if !haveResult || resultErr != nil {
				result, resultErr = r.localQuiescenceResult(ownership, durable, cleanupErr)
				haveResult = true
			}
		}
	}
	if guardianErr != nil && result.FailureText == "" {
		resultErr = errors.Join(resultErr, fmt.Errorf("wait for aborted guardian: %w", guardianErr))
	}
	if resultErr != nil && cleanupErr == nil {
		result, resultErr = r.localQuiescenceResult(ownership, durable, nil)
	}
	return result, errors.Join(cleanupErr, resultErr)
}

func (r *Runner) hasDurableQuiescenceEvidence(ownership Ownership, result Result) bool {
	if result.EvidenceSHA256 == "" || result.EvidenceJSON == "" {
		return false
	}
	evidence, digest, raw, err := ReadQuiescenceEvidence(r.runtimeRoot, ownership.GuardID)
	if err != nil || digest != result.EvidenceSHA256 || raw != result.EvidenceJSON {
		return false
	}
	return evidence.Ownership == ownership && evidence.ExitCode == result.ExitCode && evidence.ExitSignal == result.ExitSignal && evidence.Forced == result.Forced && evidence.FailureCode == result.FailureCode && evidence.FailureText == result.FailureText
}

func (r *Runner) localQuiescenceResult(ownership Ownership, durable bool, cleanupErr error) (Result, error) {
	result := Result{Ownership: ownership, Forced: true, Quiescent: cleanupErr == nil, FailureCode: "provider_cancelled", FailureText: "guardian launch was cancelled"}
	if cleanupErr != nil {
		result.FailureCode = "process_tree_unproven"
		result.FailureText = cleanupErr.Error()
		return result, cleanupErr
	}
	if !durable {
		return result, nil
	}
	result.EvidenceSHA256, result.EvidenceJSON, cleanupErr = publishEvidence(wireSpec{GuardID: ownership.GuardID, RuntimeRoot: r.runtimeRoot, Durable: true}, result)
	if cleanupErr != nil {
		result.Quiescent = false
		result.FailureCode = "quiescence_evidence_failed"
		result.FailureText = cleanupErr.Error()
	}
	return result, cleanupErr
}

type handleResult struct {
	result Result
	err    error
}

type handle struct {
	guardID     string
	ownership   Ownership
	control     *os.File
	statuses    *statusReaderHandle
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
			defer h.control.Close()
			defer h.statuses.shutdown()
			received, ok := <-h.statuses.updates
			if !ok || received.err != nil {
				err := io.EOF
				if received.err != nil {
					err = received.err
				}
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
			terminal := received.status
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
