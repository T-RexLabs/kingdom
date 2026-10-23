package opencode

import "context"

// executionLifecycle closes backend resources only after terminal snapshots
// have been read. Export is permitted only for a verified successful request.
// Physical disposal remains a separate action after canonical settlement.
type executionLifecycle interface {
	close(context.Context) error
	export(context.Context) error
	retire(context.Context, *execution) error
}

// Local execution borrows the shared server. Its existing command and native
// receipt checks close request effects; it must not stop the shared server.
type localExecutionLifecycle struct{}

func (localExecutionLifecycle) close(context.Context) error  { return nil }
func (localExecutionLifecycle) export(context.Context) error { return nil }
func (localExecutionLifecycle) retire(ctx context.Context, e *execution) error {
	return retireProfile(ctx, e.client(), e.request.directory, e.request.profile.id)
}

// Managed execution owns the entire server. Closing the handle also closes
// its credentials and tools. Stopping that server retires all its profiles.
type managedExecutionLifecycle struct{ handle ManagedHandle }

func (m managedExecutionLifecycle) close(ctx context.Context) error        { return m.handle.Close(ctx) }
func (m managedExecutionLifecycle) export(ctx context.Context) error       { return m.handle.Export(ctx) }
func (managedExecutionLifecycle) retire(context.Context, *execution) error { return nil }
