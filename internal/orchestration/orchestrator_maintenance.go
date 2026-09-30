package orchestration

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/workspace"
)

const defaultMaintenanceLimit = 4

// Durable handoffs, outboxes and cleanup markers own the work. This state only
// bounds local workers and prevents two consumers from using the same attempt.
// All fields are protected by execMu.
type maintenanceState struct {
	active           map[string]string
	changed          chan struct{}
	limit            int
	fileReplayFailed bool
	runtimeCtx       context.Context
	draining         bool
	drainError       error
	worktreeClaims   workspace.RunWorktreeCleanupClaimCursor

	// Only the admission scanner mutates cursor state; workers never do.
	scanMu            sync.Mutex
	outboxCursor      executionstore.OutboxCursor
	outboxEnd         executionstore.OutboxCursor
	fileAwaiting      bool
	nextCategory      int
	worktreeCleanupAt string
	worktreeRunID     string
}

type maintenanceRequest struct {
	kind           string
	key            string
	attemptID      string
	runID          string
	canonical      bool
	allowExecuting bool // Cancellation must be able to stop an active worker.
}

type maintenanceWorkspaceKey struct{}

func (o *orchestrationLoop) tryScheduleMaintenance(ctx context.Context, req maintenanceRequest, work func(context.Context) error) bool {
	if o == nil || ctx.Err() != nil || o.forceCloseActive() {
		return false
	}
	key := req.attemptID
	if key == "" {
		key = req.key
	}
	if key == "" {
		return false
	}
	o.execMu.Lock()
	if o.maintenance.runtimeCtx != nil {
		ctx = o.maintenance.runtimeCtx
		if ctx.Err() != nil {
			o.execMu.Unlock()
			return false
		}
	}
	limit := o.maintenance.limit
	if limit <= 0 {
		limit = defaultMaintenanceLimit
	}
	ordinary, stopping := 0, 0
	for _, kind := range o.maintenance.active {
		if kind == "cancel_stop" {
			stopping++
		} else {
			ordinary++
		}
	}
	// Stop requests are bounded control work, independent of slow filesystem
	// workers. Their subsequent restore/projection uses ordinary capacity.
	if (req.kind == "cancel_stop" && stopping >= 1) || (req.kind != "cancel_stop" && ordinary >= limit) {
		o.execMu.Unlock()
		return false
	}
	if _, exists := o.maintenance.active[key]; exists {
		o.execMu.Unlock()
		return false
	}
	if !req.allowExecuting {
		_, executing := o.inflightExec[req.attemptID]
		_, preparing := o.preparations[req.attemptID]
		if executing || preparing {
			o.execMu.Unlock()
			return false
		}
	}
	unlockWorkspace := func() {}
	if req.canonical {
		var acquired bool
		unlockWorkspace, acquired = o.tryLockRunWorkspace(req.runID)
		if !acquired {
			o.execMu.Unlock()
			return false
		}
		ctx = context.WithValue(ctx, maintenanceWorkspaceKey{}, req.runID)
	}
	if o.maintenance.active == nil {
		o.maintenance.active = make(map[string]string)
	}
	if o.maintenance.changed == nil {
		o.maintenance.changed = make(chan struct{})
	}
	o.maintenance.active[key] = req.kind
	o.execMu.Unlock()
	go func() {
		defer func() {
			unlockWorkspace()
			o.execMu.Lock()
			delete(o.maintenance.active, key)
			close(o.maintenance.changed)
			o.maintenance.changed = make(chan struct{})
			o.execMu.Unlock()
		}()
		if err := work(ctx); err != nil {
			o.execMu.Lock()
			if req.kind == "handoff_file" {
				o.maintenance.fileReplayFailed = true
			}
			if o.maintenance.draining && o.maintenance.drainError == nil && !errors.Is(err, context.Canceled) {
				switch req.kind {
				case "handoff_file", "finalize", "outbox", "cancel_stop", "cancel_finish":
					o.maintenance.drainError = err
				}
			}
			o.execMu.Unlock()
			if !errors.Is(err, context.Canceled) {
				slog.Warn("durable maintenance deferred", "kind", req.kind, "attempt_id", req.attemptID, "run_id", req.runID, "error", err)
			}
		}
	}()
	return true
}

func (o *orchestrationLoop) scheduleExecutionOutbox(ctx context.Context) (int, error) {
	if o == nil || o.executionStore == nil {
		return 0, nil
	}
	o.maintenance.scanMu.Lock()
	defer o.maintenance.scanMu.Unlock()
	if o.maintenance.outboxEnd.OutboxID == "" {
		end, err := o.executionStore.PendingOutboxHighWatermark(ctx)
		if err != nil || end.OutboxID == "" {
			return 0, err
		}
		o.maintenance.outboxEnd = end
	}
	records, err := o.executionStore.ListPendingOutboxAfter(ctx, o.maintenance.outboxCursor, o.maintenance.outboxEnd, executionOutboxPassLimit)
	if err != nil {
		return 0, err
	}
	admitted := 0
	for _, rec := range records {
		result, _, _ := decodeExecutionOutboxPayload(rec)
		canonical := executionSpecForNode(result.node.NodeID, result.node.NodeClass).WorkspaceKind == workspaceCanonical
		if o.tryScheduleMaintenance(ctx, maintenanceRequest{kind: "outbox", attemptID: rec.AttemptID, runID: rec.RunID, canonical: canonical}, func(ctx context.Context) error {
			_, err := o.projectExecutionOutboxRecord(ctx, rec)
			return err
		}) {
			admitted++
		}
		o.maintenance.outboxCursor = executionstore.OutboxCursor{CreatedAt: rec.CreatedAt, OutboxID: rec.OutboxID}
	}
	if len(records) < executionOutboxPassLimit || o.maintenance.outboxCursor == o.maintenance.outboxEnd {
		o.maintenance.outboxCursor = executionstore.OutboxCursor{}
		o.maintenance.outboxEnd = executionstore.OutboxCursor{}
	}
	return admitted, nil
}

func (o *orchestrationLoop) scheduleProviderTerminalFinalization(ctx context.Context) (int, error) {
	if o == nil || o.executionStore == nil {
		return 0, nil
	}
	o.maintenance.scanMu.Lock()
	defer o.maintenance.scanMu.Unlock()
	handoffs, err := o.executionStore.ListPendingProviderTerminalHandoffs(ctx, o.terminalHandoffCursor, providerTerminalHandoffPassLimit)
	if err != nil {
		return 0, err
	}
	admitted := 0
	for _, handoff := range handoffs {
		result, _ := decodeExecutionPayloadJSON(handoff.PayloadJSON, "terminal finalizer admission")
		canonical := executionSpecForNode(result.node.NodeID, result.node.NodeClass).WorkspaceKind == workspaceCanonical
		if o.tryScheduleMaintenance(ctx, maintenanceRequest{kind: "finalize", attemptID: handoff.AttemptID, runID: handoff.RunID, canonical: canonical}, func(ctx context.Context) error {
			return o.convergeProviderTerminalHandoff(ctx, handoff)
		}) {
			admitted++
		}
		o.terminalHandoffCursor = handoff.ProviderInvocationID
	}
	if len(handoffs) < providerTerminalHandoffPassLimit {
		o.terminalHandoffCursor = ""
	}
	return admitted, nil
}

// caughtUp requires a complete authority-ingestion scan followed by the drain
// of every admitted file replay. Snapshot finalization is separate.
func (o *orchestrationLoop) scheduleProviderTerminalHandoffFiles(ctx context.Context, limit int) (visited int, caughtUp bool, err error) {
	if o == nil || o.executionStore == nil || o.fsStore == nil {
		return 0, true, nil
	}
	o.maintenance.scanMu.Lock()
	defer o.maintenance.scanMu.Unlock()
	if o.maintenance.fileAwaiting {
		if o.maintenanceActive("handoff_file") {
			return 0, false, nil
		}
		o.execMu.Lock()
		failed := o.maintenance.fileReplayFailed
		o.maintenance.fileReplayFailed = false
		o.execMu.Unlock()
		caughtUp = !failed
		o.maintenance.fileAwaiting = false
		o.terminalHandoffFileCursor = ""
		return 0, caughtUp, nil
	}
	candidates, err := o.executionStore.ListProviderTerminalHandoffFileRecoveryCandidates(ctx, o.terminalHandoffFileCursor, limit)
	if err != nil {
		return 0, false, err
	}
	for _, candidate := range candidates {
		if !o.tryScheduleMaintenance(ctx, maintenanceRequest{kind: "handoff_file", attemptID: candidate.AttemptID, runID: candidate.RunID}, func(ctx context.Context) error {
			return o.recoverProviderTerminalHandoffFile(ctx, candidate)
		}) {
			// This scan gates lost-handoff recovery. Resume at the first
			// unadmitted record rather than skipping it and repeatedly
			// restarting a round that can never fit in local capacity.
			return visited, false, nil
		}
		o.terminalHandoffFileCursor = candidate.ProviderInvocationID
		visited++
	}
	if visited < limit {
		o.maintenance.fileAwaiting = true
		// Empty scans still wait for workers from preceding pages.
		if visited == 0 && !o.maintenanceActive("handoff_file") {
			o.execMu.Lock()
			caughtUp = !o.maintenance.fileReplayFailed
			o.maintenance.fileReplayFailed = false
			o.execMu.Unlock()
			o.maintenance.fileAwaiting = false
			o.terminalHandoffFileCursor = ""
		}
	}
	return visited, caughtUp, nil
}

func (o *orchestrationLoop) schedulePrivateWorkspaceCleanups(ctx context.Context) (int, error) {
	if o == nil || o.fsStore == nil || o.executionStore == nil {
		return 0, nil
	}
	o.maintenance.scanMu.Lock()
	defer o.maintenance.scanMu.Unlock()
	candidates, err := o.fsStore.ListPendingResponseOnlyWorkspaceCleanups(ctx, o.responseOnlyWorkspaceCleanupCursor, providerTerminalHandoffPassLimit)
	if err != nil {
		return 0, err
	}
	admitted := 0
	for _, candidate := range candidates {
		if o.tryScheduleMaintenance(ctx, maintenanceRequest{kind: "private_cleanup", attemptID: candidate.AttemptID, runID: candidate.RunID}, func(ctx context.Context) error {
			quiescent, err := o.executionStore.AttemptProcessGuardsQuiescent(ctx, candidate.AttemptID)
			if err != nil || !quiescent {
				return err
			}
			return o.removeResponseOnlyWorkspaceAndClear(ctx, candidate.RunID, candidate.AttemptID, candidate.MarkedAt)
		}) {
			admitted++
		}
		o.responseOnlyWorkspaceCleanupCursor = candidate.AttemptID
	}
	if len(candidates) < providerTerminalHandoffPassLimit {
		o.responseOnlyWorkspaceCleanupCursor = ""
	}
	return admitted, nil
}

func (o *orchestrationLoop) scheduleProviderContentCleanups(ctx context.Context) (int, error) {
	if o == nil || o.fsStore == nil || o.executionStore == nil {
		return 0, nil
	}
	o.maintenance.scanMu.Lock()
	defer o.maintenance.scanMu.Unlock()
	candidates, err := o.executionStore.ListProviderTerminalHandoffFileCleanupCandidates(ctx, o.terminalHandoffCleanupCursor, providerTerminalHandoffPassLimit)
	if err != nil {
		return 0, err
	}
	admitted := 0
	for _, candidate := range candidates {
		invocation, found, err := o.executionStore.GetProviderInvocation(ctx, candidate.ProviderInvocationID)
		if err != nil {
			return admitted, err
		}
		if found && o.tryScheduleMaintenance(ctx, maintenanceRequest{kind: "content_cleanup", attemptID: invocation.AttemptID, runID: invocation.RunID}, func(ctx context.Context) error {
			return o.cleanupProviderTerminalHandoffFile(ctx, candidate)
		}) {
			admitted++
		}
		o.terminalHandoffCleanupCursor = candidate.ProviderInvocationID
	}
	if len(candidates) < providerTerminalHandoffPassLimit {
		o.terminalHandoffCleanupCursor = ""
	}
	return admitted, nil
}

func (o *orchestrationLoop) scheduleAttemptCancellations(ctx context.Context) (int, error) {
	if o == nil || o.executionStore == nil {
		return 0, nil
	}
	o.attemptCancelMu.Lock()
	defer o.attemptCancelMu.Unlock()
	const limit = 64
	if o.attemptCancelHighWatermark.CancelOutboxID == "" {
		end, err := o.executionStore.PendingAttemptCancelHighWatermark(ctx)
		if err != nil || end.CancelOutboxID == "" {
			return 0, err
		}
		o.attemptCancelHighWatermark = end
	}
	records, err := o.executionStore.ListPendingAttemptCancels(ctx, o.attemptCancelCursor, o.attemptCancelHighWatermark, limit)
	if err != nil {
		return 0, err
	}
	admitted := 0
	for _, rec := range records {
		o.cancelExecutionPreparation(rec.AttemptID)
		quiescent, err := o.executionStore.AttemptProcessGuardsQuiescent(ctx, rec.AttemptID)
		if err != nil {
			return admitted, err
		}
		ready := quiescent || rec.ExecutionMode != executionstore.ExecutionModeProviderBacked || strings.TrimSpace(rec.SessionID) == "" || rec.HandoffState == "guardian_evidence_recovery_requested"
		request := maintenanceRequest{kind: "cancel_stop", attemptID: rec.AttemptID, runID: rec.RunID, allowExecuting: true}
		if ready {
			request.kind = "cancel_finish"
			request.allowExecuting = false
			request.canonical = executionSpecForNode(rec.NodeID, rec.NodeClass).WorkspaceKind == workspaceCanonical
		}
		if o.tryScheduleMaintenance(ctx, request, func(ctx context.Context) error {
			now := o.now().UTC().Format(time.RFC3339Nano)
			if ready {
				if rec.HandoffState == "guardian_evidence_recovery_requested" && !quiescent {
					quarantined, err := o.executionStore.AttemptProcessGuardEvidenceQuarantined(ctx, rec.AttemptID)
					if err != nil {
						return err
					}
					if !quarantined {
						return errors.New("guardian evidence recovery no longer has a durable absence quarantine")
					}
				}
				_, err := o.completeAttemptCancelHandoff(ctx, rec, now)
				return err
			}
			stopped, err := o.requestAttemptCancelStop(ctx, &rec, now)
			if err != nil || !stopped {
				return err
			}
			// Release the stop slot before any trusted workspace restoration.
			// The existing durable phase keeps completion eligible for admission.
			return o.recordAttemptCancelFailureMessage(ctx, rec, "Kingdom is waiting for cancellation workspace completion.", "awaiting_workspace_quiescence", o.now().UTC().Format(time.RFC3339Nano))
		}) {
			admitted++
		}
		o.attemptCancelCursor = executionstore.AttemptCancelCursor{CreatedAt: rec.CreatedAt, CancelOutboxID: rec.CancelOutboxID}
	}
	if len(records) < limit || o.attemptCancelCursor == o.attemptCancelHighWatermark {
		o.attemptCancelCursor = executionstore.AttemptCancelCursor{}
		o.attemptCancelHighWatermark = executionstore.AttemptCancelCursor{}
	}
	return admitted, nil
}

func (o *orchestrationLoop) scheduleWorktreeClaimRecovery(ctx context.Context) (int, error) {
	if o == nil || o.runWorktrees == nil {
		return 0, nil
	}
	// Identity inspection belongs to a bounded worker, not the admission loop.
	// The singleton key serializes cursor updates; claims retain their own CAS
	// fence and are never reclaimed while the exact owning process is alive.
	if !o.tryScheduleMaintenance(ctx, maintenanceRequest{kind: "worktree_claim_recovery", key: "worktree_claim_recovery"}, func(ctx context.Context) error {
		o.execMu.Lock()
		cursor := o.maintenance.worktreeClaims
		o.execMu.Unlock()
		next, err := o.runWorktrees.ReclaimAbandonedCleanupClaimsBatch(ctx, o.now().UTC().Format(time.RFC3339), cursor, runWorktreeCleanupBatchSize)
		o.execMu.Lock()
		o.maintenance.worktreeClaims = next
		o.execMu.Unlock()
		return err
	}) {
		return 0, nil
	}
	return 1, nil
}

func (o *orchestrationLoop) scheduleWorktreeCleanups(ctx context.Context) (int, error) {
	if o == nil || o.runWorktrees == nil {
		return 0, nil
	}
	o.maintenance.scanMu.Lock()
	defer o.maintenance.scanMu.Unlock()
	now := o.now().UTC().Format(time.RFC3339)
	records, err := o.runWorktrees.ListDueForCleanupAfter(ctx, now, o.maintenance.worktreeCleanupAt, o.maintenance.worktreeRunID, runWorktreeCleanupBatchSize)
	if err != nil {
		return 0, err
	}
	admitted := 0
	for _, record := range records {
		if o.tryScheduleMaintenance(ctx, maintenanceRequest{kind: "worktree_cleanup", key: "worktree_cleanup:" + record.RunID, runID: record.RunID, canonical: true}, func(ctx context.Context) error {
			_, err := o.cleanupRunWorktree(ctx, record, o.now().UTC().Format(time.RFC3339))
			return err
		}) {
			admitted++
		}
		o.maintenance.worktreeCleanupAt = record.CleanupAfter.String
		o.maintenance.worktreeRunID = record.RunID
	}
	if len(records) < runWorktreeCleanupBatchSize {
		o.maintenance.worktreeCleanupAt, o.maintenance.worktreeRunID = "", ""
	}
	return admitted, nil
}

func (o *orchestrationLoop) scheduleMaintenancePass(ctx context.Context) error {
	// Rotate category priority so a stream of finalizations cannot monopolize
	// all local capacity ahead of cancellation or cleanup.
	scans := []func(context.Context) (int, error){
		o.scheduleProviderTerminalFinalization,
		o.scheduleExecutionOutbox,
		o.scheduleAttemptCancellations,
		o.schedulePrivateWorkspaceCleanups,
		o.scheduleProviderContentCleanups,
		o.scheduleWorktreeClaimRecovery,
		o.scheduleWorktreeCleanups,
	}
	o.maintenance.scanMu.Lock()
	start := o.maintenance.nextCategory
	o.maintenance.nextCategory = (start + 1) % len(scans)
	o.maintenance.scanMu.Unlock()
	var scanErr error
	for i := range scans {
		_, err := scans[(start+i)%len(scans)](ctx)
		scanErr = errors.Join(scanErr, err)
	}
	return scanErr
}

func (o *orchestrationLoop) maintenanceActive(kind string) bool {
	o.execMu.Lock()
	defer o.execMu.Unlock()
	for _, activeKind := range o.maintenance.active {
		if kind == "" || kind == activeKind {
			return true
		}
	}
	return false
}

// A worker admitted with the canonical reservation must not acquire it again.
// Direct recovery callers also use a nonblocking reservation, leaving their
// durable record pending when another consumer owns the run.
func (o *orchestrationLoop) tryReserveCompletionWorkspace(ctx context.Context, runID string) (func(), bool) {
	if reserved, _ := ctx.Value(maintenanceWorkspaceKey{}).(string); reserved != "" && reserved == strings.TrimSpace(runID) {
		return func() {}, true
	}
	return o.tryLockRunWorkspace(runID)
}

// Dispatch/provider workers have stopped before this drain. Admit remaining
// durable completion work while waiting, rather than waiting for workers that
// have not yet been admitted. Cleanup debt can safely resume next startup.
func (r *Runtime) drainMaintenance(ctx context.Context) error {
	if r == nil || r.orchestrator == nil || r.orchestrator.executionStore == nil {
		return nil
	}
	o := r.orchestrator
	if err := r.ensureMaintenanceContext(ctx); err != nil {
		return err
	}
	o.execMu.Lock()
	o.maintenance.draining, o.maintenance.drainError = true, nil
	o.execMu.Unlock()
	defer func() {
		o.execMu.Lock()
		o.maintenance.draining = false
		o.execMu.Unlock()
	}()
	for {
		if o.forceCloseActive() {
			return context.Canceled
		}
		o.execMu.Lock()
		drainErr := o.maintenance.drainError
		o.execMu.Unlock()
		if drainErr != nil {
			return drainErr
		}
		_, filesCaughtUp, err := o.scheduleProviderTerminalHandoffFiles(ctx, providerTerminalHandoffPassLimit)
		if err != nil {
			return err
		}
		if _, err := o.scheduleProviderTerminalFinalization(ctx); err != nil {
			return err
		}
		if _, err := o.scheduleExecutionOutbox(ctx); err != nil {
			return err
		}
		if _, err := o.scheduleAttemptCancellations(ctx); err != nil {
			return err
		}
		handoffs, err := o.executionStore.ListPendingProviderTerminalHandoffs(ctx, "", 1)
		if err != nil {
			return err
		}
		outboxPending, err := r.hasPendingExecutionOutbox(ctx)
		if err != nil {
			return err
		}
		cancelEnd, err := o.executionStore.PendingAttemptCancelHighWatermark(ctx)
		if err != nil {
			return err
		}
		if filesCaughtUp && len(handoffs) == 0 && !outboxPending && cancelEnd.CancelOutboxID == "" && !o.maintenanceActive("") {
			return nil
		}
		if err := sleepWithContext(ctx, nil, r.completionProjectionInterval()); err != nil {
			return err
		}
	}
}
