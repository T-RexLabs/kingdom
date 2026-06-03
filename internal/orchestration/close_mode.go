package orchestration

import "sync/atomic"

// runtimeCloseMode carries the runtime-owned distinction between
// graceful and forced close. Graceful paths may finish bounded
// detached cleanup; forced paths must stand that work down so summon can
// return immediately after the operator escalates.
type runtimeCloseMode struct {
	forced atomic.Bool
}

func newRuntimeCloseMode() *runtimeCloseMode {
	return &runtimeCloseMode{}
}

func (m *runtimeCloseMode) MarkForced() {
	if m != nil {
		m.forced.Store(true)
	}
}

func (m *runtimeCloseMode) Reset() {
	if m != nil {
		m.forced.Store(false)
	}
}

func (m *runtimeCloseMode) IsForced() bool {
	return m != nil && m.forced.Load()
}
