package orchestration

import (
	"strings"
	"time"
)

func (o *orchestrationLoop) providerTimeoutMillisForNode(nodeID, nodeClass string) int64 {
	timeout := o.providerTimeoutForNode(nodeID, nodeClass)
	if timeout <= 0 {
		return 0
	}
	return timeout.Milliseconds()
}

func (o *orchestrationLoop) providerTimeoutForNode(nodeID, nodeClass string) time.Duration {
	if o == nil {
		return 0
	}
	if timeout, ok := o.nodeTimeouts[strings.TrimSpace(nodeID)]; ok && timeout > 0 {
		return timeout
	}
	classID := resolvedNodeClass(nodeID, nodeClass)
	if timeout, ok := o.nodeTimeouts[classID]; ok && timeout > 0 {
		return timeout
	}
	return 0
}

func (o *orchestrationLoop) providerExecutionLeaseDuration(nodeID, nodeClass string) time.Duration {
	// The fence covers the configured wall-clock budget and the complete quiet
	// cancellation path, but does not introduce heartbeat or stale-handoff work.
	leaseDuration := o.providerTimeoutForNode(nodeID, nodeClass)
	for _, allowance := range []time.Duration{o.streamInactivity, o.cancelGrace, o.cancelForce} {
		if allowance > 0 {
			leaseDuration += allowance
		}
	}
	return leaseDuration
}
