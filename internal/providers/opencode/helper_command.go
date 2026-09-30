package opencode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"kingdom/internal/processguardian"
)

// HelperCommandOutput owns complete disk-spooled command streams. Callers read
// them incrementally and Close after use. DiagnosticError transfers ownership
// to the returned error so failed commands retain their full diagnostics.
type HelperCommandOutput struct {
	stdout capturedOutput
	stderr capturedOutput
}

func RunHelperCommand(ctx context.Context, runner GuardianRunner, command processguardian.Command) (*HelperCommandOutput, error) {
	stdout, stderr, _, err := runGuardedCommand(ctx, runner, command)
	return &HelperCommandOutput{stdout: stdout, stderr: stderr}, err
}

func CollectHelperCommandOutput(ctx context.Context, handle processguardian.Handle) (*HelperCommandOutput, error) {
	if handle == nil {
		return nil, fmt.Errorf("helper command process handle is required")
	}
	stdout, stderr, _, err := collectGuardedCommandWithLifecycle(ctx, handle, false, 0)
	return &HelperCommandOutput{stdout: stdout, stderr: stderr}, err
}

func (o *HelperCommandOutput) OpenStdout() (io.ReadCloser, error) {
	return os.Open(o.stdout.Path)
}

func (o *HelperCommandOutput) OpenStderr() (io.ReadCloser, error) {
	return os.Open(o.stderr.Path)
}

func (o *HelperCommandOutput) Close() error {
	if o == nil {
		return nil
	}
	var errs []error
	for _, path := range []string{o.stdout.Path, o.stderr.Path} {
		if path != "" {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// HelperCommandError keeps a bounded display preview and paths to complete
// diagnostics. Its owner closes it when the diagnostic is retired.
type HelperCommandError struct {
	Cause  error
	Output *HelperCommandOutput
}

func (o *HelperCommandOutput) DiagnosticError(cause error) error {
	if o == nil || (o.stdout.Path == "" && o.stderr.Path == "") {
		return cause
	}
	return &HelperCommandError{Cause: cause, Output: o}
}

func (e *HelperCommandError) Error() string {
	return fmt.Sprintf("%v: %s %s (stdout %s, stderr %s)", e.Cause,
		strings.TrimSpace(string(e.Output.stdout.Preview)), strings.TrimSpace(string(e.Output.stderr.Preview)),
		e.Output.stdout.Path, e.Output.stderr.Path)
}

func (e *HelperCommandError) Unwrap() error { return e.Cause }
func (e *HelperCommandError) Close() error  { return e.Output.Close() }
