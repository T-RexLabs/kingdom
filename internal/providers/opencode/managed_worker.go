package opencode

import (
	"context"
	"net/http"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
)

// ManagedWorker provisions only an already-issued canonical invocation. The
// worker rechecks Preparation in the reservation transaction and reads its
// immutable access snapshot itself. No credentials or host paths cross this API.
// A failed provision can leave resource debt; recovery uses the same invocation.
// Returned ownership survives cancellation of the provisioning context.
type ManagedWorker interface {
	Provision(context.Context, ManagedProvision) (ManagedHandle, error)
	// CloseInvocation recovers existing ownership after uncertain provisioning.
	// It must never provision resources. Nil proves writer and capability closure;
	// final export and sandbox deletion remain separate obligations.
	CloseInvocation(context.Context, string) error
	// DiscardInvocation performs exact disposal only after canonical settlement
	// or a proven abandoned preparation. It never creates or exports resources.
	DiscardInvocation(context.Context, string) error
}

type ManagedProvision struct {
	InvocationID string
	Preparation  providerrequest.Preparation
}

// ManagedEndpoint describes one invocation-owned server. Workspace is a path
// inside the sandbox. Server is the admitted managed provider_server identity,
// not a process termination receipt. Implementations return defensive copies.
type ManagedEndpoint struct {
	Runtime   remote.Reference
	Server    *managedpb.CommandIdentity
	Workspace string
}

// ManagedHandle owns the server, its plugin authorization channel, and every
// managed command. None of these methods interprets connection loss as closure.
// The worker must retain this ownership after a caller drops the handle.
type ManagedHandle interface {
	Endpoint() ManagedEndpoint

	// Transport reaches only this server's fixed loopback port through the
	// authenticated gateway. It accepts the synthetic origin managed.invalid,
	// installs the server credential internally, and has no default dial fallback.
	// It must support streaming and request cancellation and reject redirects.
	// Its lifetime ends at Close, not when the Provision context ends.
	Transport() http.RoundTripper

	// AuthorizeRequest pins the acknowledged plugin profile and session to the
	// worker's scoped callback capability. It does not dispatch a prompt. All
	// callbacks still recheck the canonical binding and admission transactionally.
	AuthorizeRequest(context.Context, providerexecution.RequestIdentity) error

	// RecoverReceipts imports only this request's native effect receipts through
	// canonical validation. Remote file paths are never accepted as host paths.
	RecoverReceipts(context.Context, providerexecution.RequestIdentity) error

	// CancelTools fences and joins tools while the server remains available for
	// terminal log and message verification.
	CancelTools(context.Context, providerexecution.RequestIdentity) error

	// Export retains a verified immutable output after Close. It never publishes
	// that output into a trusted project or authorizes successful settlement.
	Export(context.Context) error

	// Discard performs exact disposal under the canonical disposition gate.
	Discard(context.Context) error

	// Close fences admission, cancels and joins all managed tools and the server,
	// and retains complete output and closure receipts. It works before profile
	// registration too. Error means ownership remains unresolved. Workspace export
	// is a separate fixed operation after closure, never hidden inside this call.
	// Implementations must honor the context; timeout preserves durable debt.
	Close(context.Context) error
}
