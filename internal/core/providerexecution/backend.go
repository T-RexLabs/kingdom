package providerexecution

// Backend names the execution authority that owns process closure. Remote
// allocations never inherit proof from a local PID or process guardian.
type Backend string

const (
	BackendLocal     Backend = "local"
	BackendOpenShell Backend = "openshell"
)
