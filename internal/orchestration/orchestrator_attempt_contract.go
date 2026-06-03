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
