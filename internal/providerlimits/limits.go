// Package providerlimits defines dependency-free resource bounds at provider
// process boundaries. It must not import provider, runtime, or state packages.
package providerlimits

import (
	"errors"
	"fmt"
	"io"
)

const (
	TerminalHandoffWireBytes  int64 = 32 * 1024 * 1024
	OpenCodeEventLineBytes          = 8 * 1024 * 1024
	OpenCodeEventBytes              = 32 * 1024 * 1024
	OpenCodeEventCount              = 4096
	CanonicalOutputBytes            = 4 * 1024 * 1024
	StderrTailBytes                 = 4 * 1024
	GuardedCommandStreamBytes int64 = 1 << 20
	TrackedWorkspaceEntries         = 10_000
	TrackedWorkspaceDepth           = 64
	SnapshotLineageDepth            = 64
	TrackedWorkspaceFileBytes int64 = 32 * 1024 * 1024
	TrackedWorkspaceBytes     int64 = 256 * 1024 * 1024
	StartupReplayBatchItems         = 64
	StartupReplayMaxPasses          = 4
	StartupReplayMaxItems           = StartupReplayBatchItems * StartupReplayMaxPasses
)

// LimitExceededError reports a safe provider-boundary resource failure.
// It deliberately excludes untrusted provider content.
type LimitExceededError struct {
	Resource string
	Limit    int64
}

func (e *LimitExceededError) Error() string {
	return fmt.Sprintf("provider %s exceeds %d-byte resource limit", e.Resource, e.Limit)
}

// NonRetryable marks limit violations as operator-actionable failures.
func (*LimitExceededError) NonRetryable() bool { return true }

// CountExceededError reports a safe bounded-entry resource failure.
type CountExceededError struct {
	Resource string
	Limit    int
}

func (e *CountExceededError) Error() string {
	return fmt.Sprintf("provider %s exceeds %d-entry resource limit", e.Resource, e.Limit)
}

// NonRetryable marks limit violations as operator-actionable failures.
func (*CountExceededError) NonRetryable() bool { return true }

// DepthExceededError reports a bounded workspace traversal that is too deep.
type DepthExceededError struct {
	Resource string
	Limit    int
}

func (e *DepthExceededError) Error() string {
	return fmt.Sprintf("provider %s exceeds %d-level resource limit", e.Resource, e.Limit)
}

// NonRetryable marks depth violations as operator-actionable failures.
func (*DepthExceededError) NonRetryable() bool { return true }

// IsResourceLimitError reports any typed resource-boundary violation.
func IsResourceLimitError(err error) bool {
	var byteLimit *LimitExceededError
	var countLimit *CountExceededError
	var depthLimit *DepthExceededError
	return errors.As(err, &byteLimit) || errors.As(err, &countLimit) || errors.As(err, &depthLimit)
}

// WorkspaceBudget accounts for traversed entries and tracked regular-file
// bytes before contents are read. Zero value is ready to use.
type WorkspaceBudget struct {
	entries int
	bytes   int64
}

// AddEntry reserves one filesystem entry encountered during traversal.
func (b *WorkspaceBudget) AddEntry() error {
	if b.entries >= TrackedWorkspaceEntries {
		return &CountExceededError{Resource: "tracked workspace", Limit: TrackedWorkspaceEntries}
	}
	b.entries++
	return nil
}

// RemainingEntries reports how many additional entries a bounded traversal may
// enumerate before rejecting the workspace.
func (b *WorkspaceBudget) RemainingEntries() int {
	return TrackedWorkspaceEntries - b.entries
}

// CheckDepth verifies a relative entry depth before descending into it.
func (b *WorkspaceBudget) CheckDepth(depth int) error {
	if depth > TrackedWorkspaceDepth {
		return &DepthExceededError{Resource: "tracked workspace", Limit: TrackedWorkspaceDepth}
	}
	return nil
}

// AddFile reserves one regular filesystem entry and its declared byte size.
func (b *WorkspaceBudget) AddFile(sizeBytes int64) error {
	if err := b.AddEntry(); err != nil {
		return err
	}
	if sizeBytes < 0 || sizeBytes > TrackedWorkspaceFileBytes {
		return &LimitExceededError{Resource: "tracked workspace file", Limit: TrackedWorkspaceFileBytes}
	}
	if sizeBytes > TrackedWorkspaceBytes-b.bytes {
		return &LimitExceededError{Resource: "tracked workspace", Limit: TrackedWorkspaceBytes}
	}
	b.bytes += sizeBytes
	return nil
}

// ReadBoundedOnLimit retains no more than maxBytes from reader. On the first
// overflow byte, it invokes onLimit and returns without reading further.
func ReadBoundedOnLimit(reader io.Reader, maxBytes int64, resource string, onLimit func()) ([]byte, error) {
	if maxBytes < 0 {
		maxBytes = 0
	}
	buffer := make([]byte, 0, maxBytes)
	chunk := make([]byte, 32*1024)
	for {
		readSize := len(chunk)
		if remaining := maxBytes - int64(len(buffer)); remaining < int64(readSize) {
			// Probe only one byte beyond the retained prefix so callers can
			// stop a still-running producer at the first overflow byte.
			readSize = int(remaining + 1)
		}
		count, readErr := reader.Read(chunk[:readSize])
		if count > 0 {
			remaining := maxBytes - int64(len(buffer))
			if remaining > 0 {
				keep := min(int64(count), remaining)
				buffer = append(buffer, chunk[:keep]...)
			}
			if int64(count) > remaining {
				if onLimit != nil {
					onLimit()
				}
				return buffer, &LimitExceededError{Resource: resource, Limit: maxBytes}
			}
		}
		if readErr == io.EOF {
			return buffer, nil
		}
		if readErr != nil {
			return buffer, readErr
		}
	}
}

// CopyBounded copies at most maxBytes from reader. It probes one additional
// byte so an exact boundary is accepted without consuming an unbounded input.
func CopyBounded(writer io.Writer, reader io.Reader, maxBytes int64, resource string) (int64, error) {
	if maxBytes < 0 || maxBytes == int64(^uint64(0)>>1) {
		return 0, fmt.Errorf("maximum copy size must be between 0 and %d", int64(^uint64(0)>>1)-1)
	}
	written, err := io.Copy(writer, io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return written, err
	}
	if written > maxBytes {
		return written, &LimitExceededError{Resource: resource, Limit: maxBytes}
	}
	return written, nil
}
