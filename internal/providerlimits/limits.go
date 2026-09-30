// Package providerlimits defines retained control-plane bounds and diagnostic
// preview sizes. It must not import provider, runtime, or state packages.
package providerlimits

import (
	"errors"
	"fmt"
)

const (
	StderrTailBytes         = 4 * 1024
	SnapshotLineageDepth    = 64
	StartupReplayBatchItems = 64
	StartupReplayMaxPasses  = 4
	StartupReplayMaxItems   = StartupReplayBatchItems * StartupReplayMaxPasses
)

// DepthExceededError reports a snapshot lineage that exceeds its control bound.
type DepthExceededError struct {
	Resource string
	Limit    int
}

func (e *DepthExceededError) Error() string {
	return fmt.Sprintf("provider %s exceeds %d-level resource limit", e.Resource, e.Limit)
}

func (*DepthExceededError) NonRetryable() bool { return true }

func IsResourceLimitError(err error) bool {
	var depthLimit *DepthExceededError
	return errors.As(err, &depthLimit)
}
