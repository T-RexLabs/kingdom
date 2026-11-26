package opencode

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
)

// requestRuntime holds what managed execution needs next to the server: the
// plugin gateway, command supervision, the receipt outbox and event spool.
type requestRuntime struct {
	authority *providerrequest.Authority
	commands  *commandBridge
	gateway   *gateway
	registry  *requestRegistry
	outboxDir string
	spoolDir  string
	// settleLimit bounds each step of stopping, verifying and recording a
	// request. None of them may depend on the caller's cancelled context, and
	// none counts against how long the request itself ran.
	settleLimit time.Duration
}

func newRequestRuntime(authority *providerrequest.Authority, local LocalExecutor, env []string, root string) (*requestRuntime, error) {
	outbox := filepath.Join(root, "outbox")
	spool := filepath.Join(root, "spool")
	// Captured events are only meaningful to the server process that emitted them.
	if err := os.RemoveAll(spool); err != nil {
		return nil, fmt.Errorf("reset OpenCode event spool: %w", err)
	}
	for _, directory := range []string{outbox, spool} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, fmt.Errorf("create OpenCode runtime directory: %w", err)
		}
	}
	commands := newCommandBridge(authority, local, env)
	gateway, err := newGateway(authority, commands)
	if err != nil {
		return nil, err
	}
	return &requestRuntime{authority: authority, commands: commands, gateway: gateway, registry: newRequestRegistry(), outboxDir: outbox, spoolDir: spool, settleLimit: 2 * time.Minute}, nil
}

// environment tells the Kingdom plugin where to reach the gateway. The values
// are read once by the plugin and removed from its process environment.
func (r *requestRuntime) environment(env []string, instanceID string) []string {
	env = replaceServiceEnv(env, "KINGDOM_AUTHORITY_URL", r.gateway.url())
	env = replaceServiceEnv(env, "KINGDOM_AUTHORITY_TOKEN", r.gateway.token)
	env = replaceServiceEnv(env, "KINGDOM_INSTANCE_ID", instanceID)
	return replaceServiceEnv(env, "KINGDOM_OUTBOX_DIR", r.outboxDir)
}

// recover delivers every receipt the plugin could not. It runs when the server
// starts; a receipt that cannot be delivered stays as resource debt.
func (r *requestRuntime) recover(ctx context.Context) error {
	return recoverReceipts(ctx, r.outboxDir, r.authority, nil)
}

// recoverFor delivers only the receipts that belong to one request.
func (r *requestRuntime) recoverFor(ctx context.Context, identity providerexecution.RequestIdentity) error {
	return recoverReceipts(ctx, r.outboxDir, r.authority, &identity)
}

func (r *requestRuntime) close(ctx context.Context) error {
	return r.gateway.close(ctx)
}
