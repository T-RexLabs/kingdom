package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"

	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/processguardian"
)

// recoverServerInstances proves which shared servers that earlier processes
// started have stopped, and closes what they can no longer do. Only a stopped
// server can be known to have no unreceipted write in flight, and a stopped
// server's command that never reached a guardian never ran, because a guardian
// releases a command only after its claim is still open. It returns the
// instances that are not yet proven, so a server whose guardian is still
// tearing it down is checked again on the next pass.
func (o *orchestrationLoop) recoverServerInstances(ctx context.Context) ([]string, error) {
	if o == nil || o.executionStore == nil {
		return nil, nil
	}
	instances, err := o.executionStore.OpenProviderServerInstances(ctx)
	if err != nil {
		return nil, err
	}
	var unproven []string
	for _, instance := range instances {
		closure, evidence, stopped := o.serverInstanceStopped(instance)
		if !stopped {
			unproven = append(unproven, instance.ID)
			continue
		}
		if err := o.executionStore.CloseProviderServerInstance(ctx, instance.ID, closure); err != nil {
			return unproven, fmt.Errorf("close OpenCode server instance %q: %w", instance.ID, err)
		}
		if evidence != "" {
			if err := processguardian.RemoveQuiescenceEvidence(o.guardianRuntimeRoot, instance.GuardID, evidence); err != nil && !errors.Is(err, fs.ErrNotExist) {
				slog.Warn("OpenCode server stop evidence cleanup deferred", "instance_id", instance.ID, "error", err)
			}
		}
	}
	aborted, err := o.executionStore.AbortOrphanedCommandClaims(ctx)
	if err != nil {
		return unproven, fmt.Errorf("abort command claims that never started: %w", err)
	}
	if aborted > 0 {
		slog.Info("closed command claims of stopped OpenCode servers that never started", "claims", aborted)
	}
	return unproven, nil
}

// serverInstanceStopped accepts the guardian's own evidence first, and the
// proven absence of the exact process otherwise. The server that is running now
// has neither, so it is never closed.
func (o *orchestrationLoop) serverInstanceStopped(instance providerrequest.ServerInstance) (providerrequest.ServerClosure, string, bool) {
	if strings.TrimSpace(o.guardianRuntimeRoot) != "" {
		if result, _, evidence, err := processguardian.ReadQuiescenceEvidence(o.guardianRuntimeRoot, instance.GuardID); err == nil && result.Quiescent {
			return providerrequest.ClosedByGuardian, evidence, true
		}
	}
	var ownership processguardian.Ownership
	if err := json.Unmarshal([]byte(instance.OwnershipJSON), &ownership); err != nil {
		return "", "", false
	}
	absent := processguardian.ManagedProcessAbsent
	if o.serverAbsent != nil {
		absent = o.serverAbsent
	}
	if absent(ownership.ChildPID, ownership.TreeID, ownership.ChildIdentityToken, ownership.TreeIdentityToken) {
		return providerrequest.ClosedByAbsence, "", true
	}
	return "", "", false
}
