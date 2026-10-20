package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

const defaultPreparationLimit = 4

// Preparation owns the existing queued execution lease and local drain token.
// The token transfers to the provider worker only once launch is authorized.
type providerAttemptPreparation struct {
	parentCtx       context.Context
	ctx             context.Context
	cancel          context.CancelFunc
	heartbeat       *providerLeaseHeartbeat
	transferred     bool
	unlockWorkspace func()
	leaseOwner      string
	leaseVersion    int64
	leaseExpiresAt  string
	leaseDuration   time.Duration
}

func (o *orchestrationLoop) dispatchNodeAttempt(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, now string) error {
	ctx = firstNonNilContext(ctx)
	pending, err := o.runHasPendingTerminalHandoff(ctx, run.RunID)
	if err != nil || pending {
		return err
	}
	autoSucceeded, err := o.maybeAutoSucceedLoopCappedReadyNode(ctx, run, node, attempt, now)
	if err != nil || autoSucceeded {
		return err
	}
	spec := executionSpecForNode(node.NodeID, node.NodeClass)
	if spec.Mode != executionstore.ExecutionModeProviderBacked {
		return o.prepareNodeAttempt(ctx, run, node, attempt, now, nil)
	}
	fence, err := o.loadExecutionCompletionFence(ctx, run.RunID, attempt.NodeRunID)
	if err != nil {
		return err
	}
	if !fence.found || fence.runStatus != "running" || fence.nodeStatus != "ready" {
		return nil
	}
	unlock := func() {}
	if spec.WorkspaceKind == workspaceCanonical {
		var acquired bool
		unlock, acquired = o.tryLockRunWorkspace(run.RunID)
		if !acquired {
			return nil
		}
	}
	attemptID := composeAttemptID(run.RunID, attempt.NodeID, attempt.Attempt)
	o.execMu.Lock()
	limit := o.preparationLimit
	if limit <= 0 {
		limit = defaultPreparationLimit
	}
	_, inflight := o.inflightExec[attemptID]
	_, preparing := o.preparations[attemptID]
	if inflight || preparing || len(o.preparations) >= limit || ctx.Err() != nil || o.forceCloseActive() {
		o.execMu.Unlock()
		unlock()
		return nil
	}
	if o.preparations == nil {
		o.preparations = make(map[string]*providerAttemptPreparation)
	}
	prepCtx, cancel := context.WithCancel(ctx)
	prep := &providerAttemptPreparation{parentCtx: ctx, ctx: prepCtx, cancel: cancel, unlockWorkspace: unlock}
	o.preparations[attemptID] = prep
	o.execMu.Unlock()
	if !o.markExecutionInflight(attemptID, run.RunID) {
		o.finishPreparation(attemptID, prep)
		return nil
	}
	admitted := false
	defer func() {
		if !admitted {
			o.finishPreparation(attemptID, prep)
			o.clearExecutionInflight(attemptID)
		}
	}()
	attempt, err = o.enqueueDispatchAttempt(prep.ctx, run, node, attempt, now)
	if err != nil {
		return err
	}
	owner, err := generateExecutionLeaseOwner()
	if err != nil {
		return err
	}
	prep.leaseDuration = o.providerExecutionLeaseDuration(node.NodeID, node.NodeClass)
	if prep.leaseDuration <= 0 {
		return fmt.Errorf("provider execution lease duration is required for node %q", node.NodeID)
	}
	claimedAt := o.now().UTC()
	claim, acquired, err := o.executionStore.ClaimQueuedProviderAttempt(prep.ctx, executionstore.ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: owner,
		LeaseUntil: claimedAt.Add(prep.leaseDuration).Format(time.RFC3339Nano),
		ClaimedAt:  claimedAt.Format(time.RFC3339Nano),
	})
	if err != nil || !acquired {
		return err
	}
	attempt.DurableState, attempt.LeaseOwner, attempt.LeaseVersion = claim.State, claim.LeaseOwner, claim.LeaseVersion
	prep.leaseOwner, prep.leaseVersion, prep.leaseExpiresAt = claim.LeaseOwner, claim.LeaseVersion, claim.LeaseExpiresAt
	if err := o.pinAttemptWorkspaceInput(prep.ctx, run, node, attempt, attemptID); err != nil {
		if errors.Is(err, errWorkspaceBusy) {
			return nil
		}
		return o.failNodeAttemptSetup(prep.ctx, run, node, attempt, attemptID, "pin workspace input", err)
	}
	prep.heartbeat = o.startProviderLeaseHeartbeat(prep.ctx, prep.cancel, asyncExecutionResult{
		attempt:                attempt,
		attemptID:              attemptID,
		run:                    run,
		node:                   node,
		expectedLeaseOwner:     prep.leaseOwner,
		expectedLeaseVersion:   prep.leaseVersion,
		expectedLeaseExpiresAt: prep.leaseExpiresAt,
		providerLeaseDuration:  prep.leaseDuration,
	})
	o.emitPreparationProgress(prep.ctx, run, node, attempt, attemptID, "preparation_admitted", preparedNodeWorkspace{})
	o.execMu.Lock()
	o.lastPreparationRunID = run.RunID
	o.execMu.Unlock()
	admitted = true
	go func() {
		defer func() {
			o.finishPreparation(attemptID, prep)
			if !prep.transferred {
				o.clearExecutionInflight(attemptID)
			}
		}()
		if err := o.prepareNodeAttempt(prep.ctx, run, node, attempt, o.now().UTC().Format(time.RFC3339Nano), prep); err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("provider execution preparation failed", "attempt_id", attemptID, "error", err)
		}
	}()
	return nil
}

func (o *orchestrationLoop) emitPreparationProgress(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID, phase string, workspace preparedNodeWorkspace) {
	if o.events == nil {
		return
	}
	payload, err := marshalExecutionAttemptEventPayload(attemptID, node.NodeID, attempt.NodeRunID, attempt.Attempt, phase, map[string]any{
		"phase":                 phase,
		"workspace_kind":        executionSpecForNode(node.NodeID, node.NodeClass).WorkspaceKind,
		"workspace_path":        workspace.WorkspacePath,
		"project_root_boundary": workspace.ProjectRootBoundary,
		"base_snapshot_id":      workspace.BaseSnapshotID,
		"lease_version":         attempt.LeaseVersion,
	})
	if err != nil {
		return
	}
	_, err = o.events.Ingest(ctx, state.AttemptEventIngestionRequest{
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_%s_%d", sanitizeFileToken(run.RunID), sanitizeFileToken(node.NodeID), attempt.Attempt, phase, attempt.LeaseVersion),
		RunID:           run.RunID,
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: true},
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.progress",
		Seq:             o.nextEventSeq(ctx, attemptID),
		PayloadJSON:     payload,
		ActorID:         "runtime_orchestrator",
		RoleID:          node.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      phase,
		ReasonText:      strings.ReplaceAll(phase, "_", " "),
		CorrelationID:   "corr_orch_" + run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_%s", run.RunID, attempt.NodeRunID, phase),
		CreatedAt:       o.now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("failed to record preparation progress", "attempt_id", attemptID, "phase", phase, "error", err)
		}
		return
	}
	// Preparation happens before a provider invocation exists. Record it in the
	// same durable activity stream so live and reopened conversations agree.
	o.emitProviderActivityObservation(ctx, asyncExecutionResult{
		run: run, node: node, attempt: attempt, attemptID: attemptID,
	}, opencode.ExecuteActivityObservation{Kind: "phase", Label: phase})
}

// Admission persists only execution metadata. Prompt and filesystem work belong
// to the bounded worker after its queued lease and local drain token exist.
func (o *orchestrationLoop) enqueueDispatchAttempt(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, now string) (nodeAttempt, error) {
	if o.executionStore == nil {
		return attempt, fmt.Errorf("execution store is required for dispatch")
	}
	spec := executionSpecForNode(node.NodeID, node.NodeClass)
	attemptID := composeAttemptID(run.RunID, attempt.NodeID, attempt.Attempt)
	attempt.ExecutionMode, attempt.CompletionMode = spec.Mode, spec.CompletionMode
	if spec.Mode == executionstore.ExecutionModeProviderBacked {
		attempt.SessionRef = sessionIDForAttempt(attemptID)
		// An invalid checkpoint fails the attempt during preparation, where the
		// reason is recorded; admission must not wedge the node in ready.
		if resume, err := o.loadDispatchResume(ctx, attempt.NodeRunID); err == nil && resume != nil {
			attempt.SessionRef = resume.Binding.LogicalSessionID
		}
		attempt.ProviderTimeoutMillis = o.providerTimeoutMillisForNode(node.NodeID, node.NodeClass)
	}
	err := o.executionStore.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{
		AttemptID:             attemptID,
		RunID:                 run.RunID,
		NodeRunID:             attempt.NodeRunID,
		ExecutionMode:         spec.Mode,
		CompletionMode:        spec.CompletionMode,
		ProviderTimeoutMillis: attempt.ProviderTimeoutMillis,
		SessionRef:            attempt.SessionRef,
		CreatedAt:             now,
	})
	if err != nil {
		return attempt, fmt.Errorf("enqueue durable attempt execution %q: %w", attemptID, err)
	}
	return attempt, nil
}

func (o *orchestrationLoop) finishPreparation(attemptID string, prep *providerAttemptPreparation) {
	if !prep.transferred {
		prep.cancel()
	}
	if prep.heartbeat != nil {
		prep.heartbeat.stopAndWait()
		prep.heartbeat = nil
	}
	if !prep.transferred && prep.leaseOwner != "" {
		ctx, cancel := detachedTimeoutContext(prep.parentCtx, 5*time.Second)
		defer cancel()
		if err := o.executionStore.ReleaseQueuedProviderAttemptLease(ctx, attemptID, prep.leaseOwner, prep.leaseVersion, o.now().UTC().Format(time.RFC3339Nano)); err != nil {
			slog.Warn("failed to release unlaunched preparation lease", "attempt_id", attemptID, "error", err)
		}
	}
	prep.unlockWorkspace()
	o.execMu.Lock()
	delete(o.preparations, attemptID)
	o.execMu.Unlock()
}

// Cancellation does not need a provider session in order to stop queued work.
func (o *orchestrationLoop) cancelExecutionPreparation(attemptID string) {
	o.execMu.Lock()
	prep := o.preparations[attemptID]
	o.execMu.Unlock()
	if prep != nil {
		prep.cancel()
	}
}

func (o *orchestrationLoop) preparationMayLaunch(ctx context.Context, job *asyncExecutionResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fence, err := o.loadExecutionCompletionFence(ctx, job.run.RunID, job.attempt.NodeRunID)
	if err != nil {
		return err
	}
	if !fence.found || fence.runStatus != "running" || (fence.nodeStatus != "ready" && fence.nodeStatus != "starting") {
		return context.Canceled
	}
	renewed, expiresAt, err := o.renewProviderExecutionLease(ctx, *job, job.expectedLeaseExpiresAt)
	if err != nil {
		return err
	}
	if !renewed {
		return fmt.Errorf("provider preparation lost its execution lease")
	}
	job.expectedLeaseExpiresAt = expiresAt
	return nil
}
