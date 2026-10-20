package orchestration

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

func TestFileOnlyLocalFailureWithoutProviderObservationRecoversAndRestoresBuilderWorkspace(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	ctx := context.Background()
	invocation := h.addInvocation(t, 1)
	job := h.runningHandoffJob(t, invocation, 1)
	binding := h.requests.issueDispatch(t, invocation.ProviderInvocationID)
	if err := h.requests.authority.CloseAdmission(ctx, binding.Identity); err != nil {
		t.Fatal(err)
	}
	job.outcome = opencode.ExecutionOutcome{
		State: "failed", ReasonCode: "internal_error",
		FailureText: "finalize provider messages: HTTP 400", EvidenceSource: "adapter",
	}
	job.err = errorFromExecutionOutcome(job.outcome)
	job.providerDiagnosticsJSON = `{"operation":"finalize_messages","status":400}`
	target, err := h.fsStore.GetExecutionTarget(ctx, invocation.RunID)
	if err != nil {
		t.Fatal(err)
	}
	job.workspacePath = target.ExecutionRoot
	job.baseSnapshotID = h.baselineSnapshotID
	job.delegator = job.attempt.AgentID
	h.loop.mailbox = state.NewMailboxStore(h.db, h.loop.schemas)
	// Canonical dispatch marks the target dirty before the builder may edit it.
	if err := h.fsStore.UpdateExecutionTargetState(ctx, invocation.RunID, "attempt_dirty", target.TrustedSnapshotID, "", terminalHandoffFixtureNow); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target.ExecutionRoot, "README.md"), []byte("unverified failed builder changes\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := h.loop.markProviderTerminalHandoffPending(ctx, job); err != nil {
		t.Fatal(err)
	}
	persistHandoffFileOnlyFromJob(t, h, job, "2026-03-01T12:00:04Z")
	file, found, err := h.fsStore.ReadProviderTerminalHandoff(invocation.ProviderInvocationID)
	if err != nil || !found {
		t.Fatalf("local failure file missing: found=%t err=%v", found, err)
	}
	wire, err := decodeProviderTerminalHandoffWire(file.Wire)
	if err != nil || wire.StreamObservation.Request != nil {
		t.Fatalf("fixture unexpectedly has provider terminal evidence: wire=%#v err=%v", wire, err)
	}
	if count := countTestRows(t, h.db, `SELECT COUNT(*) FROM provider_terminal_handoffs`); count != 0 {
		t.Fatalf("file-only fixture has %d database handoffs", count)
	}
	mustExec(t, h.db, `UPDATE attempt_executions SET lease_expires_at='2026-03-01T12:00:05Z'`)
	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 10, 0, 0, time.UTC) }

	// Startup uses the same file ingestion and asynchronous settlement workers
	// as a real restart; it must not ask the model to reconstruct an answer.
	replayStartupHandoffsThroughMaintenance(t, h)
	settlement, found, err := h.store.GetProviderCompletionSettlementByInvocation(ctx, invocation.ProviderInvocationID)
	if err != nil || !found || settlement.Outcome != executionstore.ProviderCompletionOutcomeFailed || settlement.FailureReasonCode != "internal_error" || settlement.FailureReasonText != job.outcome.FailureText {
		t.Fatalf("local failure did not settle with its original cause: found=%t settlement=%#v err=%v", found, settlement, err)
	}
	if settlement.ProviderDiagnosticsJSON != job.providerDiagnosticsJSON {
		t.Fatalf("original diagnostics changed: got=%s", settlement.ProviderDiagnosticsJSON)
	}
	attempt, found, err := h.store.GetAttempt(ctx, invocation.AttemptID)
	if err != nil || !found || attempt.State != "committed_failure" {
		t.Fatalf("attempt remains unsettled: found=%t attempt=%#v err=%v", found, attempt, err)
	}
	evidence, err := h.store.ListProviderCompletionEvidence(ctx, invocation.ProviderInvocationID)
	if err != nil || len(evidence) != 1 {
		t.Fatalf("canonical failure evidence: count=%d err=%v", len(evidence), err)
	}
	var envelope providerTerminalHandoffStreamEnvelope
	if err := json.Unmarshal([]byte(evidence[0].PayloadJSON), &envelope); err != nil {
		t.Fatal(err)
	}
	original, err := json.Marshal(wire.StreamObservation)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := json.Marshal(envelope.Observation)
	if err != nil || string(persisted) != string(original) {
		t.Fatalf("failure evidence was replaced by synthesized provider evidence: original=%s persisted=%s err=%v", original, persisted, err)
	}
	request, found, err := providerrequest.ForOwner(ctx, h.db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: invocation.ProviderInvocationID})
	if err != nil || !found || request.TerminalReference == nil {
		t.Fatalf("abandoned request is not durably closed: found=%t request=%#v err=%v", found, request, err)
	}

	abandoned, err := providerexecution.AbandonedRequestTerminal(invocation.ProviderInvocationID).Reference()
	if err != nil || *request.TerminalReference != abandoned {
		t.Fatalf("request closure fabricated a provider answer: terminal=%#v err=%v", request.TerminalReference, err)
	}

	projectionCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if admitted, err := h.loop.scheduleExecutionOutbox(projectionCtx); err != nil || admitted != 1 {
		t.Fatalf("failure projection admission: admitted=%d err=%v", admitted, err)
	}
	if err := h.loop.waitForMaintenance(projectionCtx); err != nil {
		t.Fatal(err)
	}
	var outboxStatus, outboxError string
	if err := h.db.QueryRow(`SELECT status, COALESCE(last_error, '') FROM attempt_outbox WHERE attempt_id=?`, invocation.AttemptID).Scan(&outboxStatus, &outboxError); err != nil || outboxStatus != "published" {
		t.Fatalf("failure projection did not converge: status=%s last_error=%s err=%v", outboxStatus, outboxError, err)
	}
	for _, root := range []string{target.ExecutionRoot, h.projectRoot} {
		text, err := os.ReadFile(filepath.Join(root, "README.md"))
		if err != nil || string(text) != "trusted baseline\n" {
			t.Fatalf("failed builder changes escaped recovery at %s: content=%q err=%v", root, text, err)
		}
	}
	after, err := h.fsStore.GetExecutionTarget(ctx, invocation.RunID)
	if err != nil || after.TrustedSnapshotID != h.baselineSnapshotID || after.PublishedSnapshotID != h.baselineSnapshotID {
		t.Fatalf("failed builder snapshot was promoted: target=%#v err=%v", after, err)
	}
	// A second startup pass is idempotent even though the original file has
	// already been consumed and the provider request now has abandonment proof.
	replayStartupHandoffsThroughMaintenance(t, h)
	if count := countTestRows(t, h.db, `SELECT COUNT(*) FROM provider_completion_settlements`); count != 1 {
		t.Fatalf("recovery settled %d times", count)
	}
	if executions := h.requests.executions(invocation.ProviderInvocationID); executions != 0 {
		t.Fatalf("recovery dispatched the model %d times", executions)
	}
	if _, found, err := h.fsStore.ReadProviderTerminalHandoff(invocation.ProviderInvocationID); err != nil || found {
		t.Fatalf("consumed failure file remains: found=%t err=%v", found, err)
	}
}
