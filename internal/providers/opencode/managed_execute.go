package opencode

import (
	"context"
	"errors"
	"fmt"

	"kingdom/internal/core/providerexecution"
)

// Execute uses the same protocol verifier and terminal handoff as local
// execution. The canonical authority still controls whether remote dispatch is
// enabled; preparation and an available transport cannot bypass that gate.
func (s *ManagedService) Execute(ctx context.Context, req ExecuteRequest) (response ExecuteResponse, err error) {
	s.mu.Lock()
	entry := s.entries[req.InvocationID]
	s.mu.Unlock()
	if entry == nil {
		return response, fmt.Errorf("managed invocation was not prepared")
	}
	select {
	case <-entry.ready:
	case <-ctx.Done():
		return response, ctx.Err()
	}
	prepared := entry.request
	if prepared == nil || entry.attempt != req.AttemptID {
		return response, errors.New("managed request does not match preparation")
	}
	if req.OnCommandGuardianOwnership != nil || req.OnCommandGuardianQuiescence != nil {
		return response, errors.New("managed execution cannot register local command guardians")
	}
	prepared.mu.Lock()
	if prepared.executing || prepared.stopped {
		prepared.mu.Unlock()
		return response, errors.New("managed invocation is executing or stopped")
	}
	prepared.executing = true
	prepared.mu.Unlock()
	defer func() {
		// All execution defers (including capture readers) have completed before
		// releasing cancellation waiters or permitting capture disposal.
		close(prepared.done)
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.stopLimit)
		defer cancel()
		err = errors.Join(err, s.closeEntry(cleanup, entry))
	}()
	runtime := &executionRuntime{
		authority: s.authority, settleLimit: s.stopLimit,
		commands:   managedExecutionCommands{handle: entry.handle},
		recoverFor: entry.handle.RecoverReceipts,
		lifecycle:  managedExecutionLifecycle{handle: entry.handle},
	}
	return (&execution{runtime: runtime, request: prepared, req: req}).execute(ctx)
}

type managedExecutionCommands struct{ handle ManagedHandle }

func (managedExecutionCommands) register(providerexecution.RequestIdentity, *commandRegistration) error {
	return errors.New("managed execution cannot register local command guardians")
}
func (managedExecutionCommands) unregister(providerexecution.RequestIdentity) {}
func (c managedExecutionCommands) cancel(ctx context.Context, identity providerexecution.RequestIdentity) error {
	return c.handle.CancelTools(ctx, identity)
}
