package orchestration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/core/schemas"
	"kingdom/internal/execution/providerrequest"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/processguardian"
	"kingdom/internal/providerlimits"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
)

func TestLargeContentFileOnlyHandoffReplaysAfterCrashAndLeaseExpiry(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	job := h.runningHandoffJob(t, invocation, 1)
	text := strings.Repeat("terminal response\n", 2000000)
	if len(text) <= 32*1024*1024 {
		t.Fatal("fixture must cross old handoff ceiling")
	}
	h.configureSuccessfulHandoff(t, &job, text)
	if err := h.loop.markProviderTerminalHandoffPending(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	mustExec(t, h.db, `CREATE TRIGGER interrupt_content_handoff BEFORE INSERT ON provider_terminal_handoffs BEGIN SELECT RAISE(ABORT,'simulated crash before DB handoff'); END`)
	if err := h.loop.persistProviderTerminalHandoff(context.Background(), &job); err == nil {
		t.Fatal("expected interrupted DB handoff")
	}
	file, found, err := h.fsStore.ReadProviderTerminalHandoff(invocation.ProviderInvocationID)
	if err != nil || !found || len(file.Wire) > 64*1024 {
		t.Fatalf("compact durable file: found=%t bytes=%d err=%v", found, len(file.Wire), err)
	}
	wire, err := decodeProviderTerminalHandoffWire(file.Wire)
	if err != nil {
		t.Fatal(err)
	}
	ref := wire.StreamObservation.CanonicalTerminalTextRef
	if ref == nil || wire.StreamObservation.CanonicalTerminalText != "" {
		t.Fatal("terminal content remained inline")
	}
	if err := h.fsStore.CopyProviderContent(*ref, io.Discard); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := h.db.QueryRow(`SELECT count(*) FROM provider_contents`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial DB content survived crash: count=%d err=%v", count, err)
	}
	mustExec(t, h.db, `DROP TRIGGER interrupt_content_handoff`)
	mustExec(t, h.db, `UPDATE attempt_executions SET lease_expires_at='2026-03-01T12:01:00Z'`)
	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 2, 0, 0, time.UTC) }
	if err := h.loop.recoverProviderTerminalHandoffFile(context.Background(), invocation); err != nil {
		t.Fatalf("file-only replay: %v", err)
	}
	handoff, found, err := h.store.GetProviderTerminalHandoff(context.Background(), executionstore.ProviderTerminalHandoffID(invocation.ProviderInvocationID))
	if err != nil || !found || len(handoff.PayloadJSON) > 64*1024 || len(handoff.StreamObservationJSON) > 64*1024 {
		t.Fatalf("DB handoff not compact: found=%t err=%v", found, err)
	}
	// The request's events and messages are empty lists, stored once.
	if err := h.db.QueryRow(`SELECT count(*) FROM provider_contents WHERE size_bytes > ?`, len("[]")).Scan(&count); err != nil || count != 1 {
		t.Fatalf("terminal/output content duplicated: count=%d err=%v", count, err)
	}
	if err := h.fsStore.CopyProviderContent(*ref, io.Discard); err == nil {
		t.Fatal("staging content retained after canonical ingestion")
	}
	if err := h.loop.convergeProviderTerminalHandoff(context.Background(), handoff); err != nil {
		t.Fatalf("settlement replay: %v", err)
	}
	var outcome string
	var size int
	if err := h.db.QueryRow(`SELECT outcome,length(final_text) FROM provider_completion_settlements WHERE provider_invocation_id=?`, invocation.ProviderInvocationID).Scan(&outcome, &size); err != nil || outcome != "success" || size != 0 {
		t.Fatalf("settlement outcome=%s size=%d err=%v", outcome, size, err)
	}
	var payload string
	if err := h.db.QueryRow(`SELECT payload_json FROM attempt_outbox WHERE attempt_id=?`, invocation.AttemptID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if len(payload) > 64*1024 {
		t.Fatalf("outbox repeated large content: %d bytes", len(payload))
	}
	result, err := decodeExecutionPayloadJSON(payload, "large outbox")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.loop.hydrateProviderPayloadContent(context.Background(), payload, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.outputText) > 32*1024 || result.providerStreamObservation.CanonicalTerminalTextRef == nil {
		t.Fatal("outbox replay materialized canonical output")
	}
	if err := h.store.CopyProviderContent(context.Background(), *result.providerStreamObservation.CanonicalTerminalTextRef, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := h.db.QueryRow(`SELECT sum(length(content)) FROM provider_content_chunks`).Scan(&count); err != nil || count != len(strings.TrimSpace(text))+len("[]") {
		t.Fatalf("settlement did not retain exactly one content copy: bytes=%d err=%v", count, err)
	}
}

const terminalHandoffFixtureNow = "2026-03-01T12:00:00Z"

func TestRuntimeGracefulCloseWaitsForHandoffReadersToDrain(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	tracker := newExecutionDrainTracker()
	tracker.Start()
	h.loop.asyncDrain = tracker
	done := make(chan error)
	close(done)
	runtime := &Runtime{orchestrator: h.loop, started: true, periodicDone: done, dispatchDone: done, completionDone: done}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runtime.GracefulClose(ctx); err == nil {
		t.Fatal("expected incomplete drain")
	}
	tracker.Done()
	if err := runtime.GracefulClose(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReferencedResponseProjectionIsCompleteAndIdempotentWithOneConnection(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	h.db.SetMaxOpenConns(1)
	mustExec(t, h.db, `INSERT INTO roles(role_id,display_name,can_spawn,created_at) VALUES('coordinator','Coordinator',1,'2026-03-01T12:00:00Z')`)
	mustExec(t, h.db, `UPDATE agents SET role_id='coordinator'`)
	mustExec(t, h.db, `UPDATE workflow_nodes SET required_role='coordinator',node_class='advisory_synthesis'`)
	invocation := h.addInvocation(t, 1)
	mustExec(t, h.db, `UPDATE node_runs SET required_role='coordinator'`)
	job := h.runningHandoffJob(t, invocation, 1)
	job.node.RequiredRole = "coordinator"
	job.node.NodeClass = "advisory_synthesis"
	job.attempt.RequiredRole = "coordinator"
	body := strings.Repeat("full canonical response\n", 450000)
	h.configureSuccessfulHandoff(t, &job, body)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := h.loop.commitProviderExecutionCompletion(ctx, job); err != nil {
		t.Fatal(err)
	}
	settlement, found, err := h.store.GetProviderCompletionSettlementByInvocation(ctx, invocation.ProviderInvocationID)
	if err != nil || !found || settlement.FinalTextRef == nil || settlement.FinalText != "" {
		t.Fatalf("stream settlement=%+v err=%v", settlement, err)
	}
	job.providerStreamObservation.CanonicalTerminalText = ""
	job.providerStreamObservation.CanonicalTerminalTextRef = settlement.FinalTextRef
	job.outputText = settlement.FinalTextPreview
	h.loop.bindCanonicalTerminalReader(&job)
	for replay := range 2 {
		// Publication attempts currently use clock-derived batch identities;
		// this regression exercises content/reader replay across attempts.
		h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, replay, 0, time.UTC) }
		if err := h.loop.persistReferencedResponseSnapshot(ctx, job, terminalHandoffFixtureNow); err != nil {
			t.Fatal(err)
		}
	}
	var size int64
	if err := h.db.QueryRow(`SELECT sum(length(content)) FROM artifact_content_chunks`).Scan(&size); err != nil {
		t.Fatal(err)
	}
	want := int64(len("# Node Response Snapshot\n\n") + len(strings.TrimSpace(body)) + 1)
	if size != want {
		t.Fatalf("response artifact size=%d want=%d; preview must not become canonical artifact", size, want)
	}
	var count int
	if err := h.db.QueryRow(`SELECT count(*) FROM artifacts`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("replayed artifact count=%d err=%v", count, err)
	}
}

func TestReferencedHandoffAndSettlementReplayAfterSourceFilesAreRetired(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	job := h.runningHandoffJob(t, invocation, 1)
	body := strings.Repeat("canonical response\n", 250000)
	body = strings.TrimSpace(body)
	h.configureSuccessfulHandoff(t, &job, body)
	path := filepath.Join(t.TempDir(), "terminal")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	ref := providerexecution.ReferenceText(invocation.ProviderInvocationID, body)
	job.providerStreamObservation.CanonicalTerminalText = ""
	job.providerStreamObservation.CanonicalTerminalTextRef = &ref
	job.providerStreamObservation.StagedTerminalContent = &providerexecution.StagedContent{Reference: ref, Path: path}
	job.outputText = body[:4096]
	original := job
	if err := h.loop.markProviderTerminalHandoffPending(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if err := h.loop.persistProviderTerminalHandoff(context.Background(), &job); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("source was not retired: %v", err)
	}
	mustExec(t, h.db, `UPDATE attempt_executions SET lease_expires_at='2026-03-01T12:01:00Z'`)
	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 2, 0, 0, time.UTC) }
	replay := original
	if err := h.loop.persistProviderTerminalHandoff(context.Background(), &replay); err != nil {
		t.Fatalf("durable handoff replay required retired files: %v", err)
	}
	if err := h.loop.commitProviderExecutionCompletion(context.Background(), replay); err != nil {
		t.Fatal(err)
	}
	if err := h.loop.commitProviderExecutionCompletion(context.Background(), original); err != nil {
		t.Fatalf("canonical settlement replay required retired files: %v", err)
	}
	var count int
	if err := h.db.QueryRow(`SELECT count(*) FROM provider_completion_settlements`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("settlements=%d err=%v", count, err)
	}
}

type terminalHandoffHarness struct {
	db                 *sql.DB
	store              *executionstore.Store
	fsStore            *runtimefs.Store
	requests           *requestFixture
	loop               *orchestrationLoop
	runtimeDir         string
	projectRoot        string
	baselineSnapshotID string
}

func newTerminalHandoffHarness(t *testing.T) terminalHandoffHarness {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_terminal_handoff', 'builder', 'busy', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_terminal_handoff', 'Terminal handoff', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_terminal_handoff', 'terminal-handoff', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_terminal_handoff', 'wfdef_terminal_handoff', 'implementation', 'builder', 'delegated_execution', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_terminal_handoff', NULL, 'work_order_terminal_handoff', 'wfdef_terminal_handoff', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

	runtimeDir := t.TempDir()
	fsStore, err := runtimefs.NewStore(db, runtimeDir)
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("trusted baseline\n"), 0o644); err != nil {
		t.Fatalf("write trusted project baseline: %v", err)
	}
	baselineSnapshotID := "snap_terminal_handoff_baseline"
	baseline, err := fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{
		SnapshotID: baselineSnapshotID, RunID: "run_terminal_handoff", SnapshotKind: "baseline", RootPath: projectRoot, CreatedAt: terminalHandoffFixtureNow,
	})
	if err != nil {
		t.Fatalf("capture terminal handoff baseline: %v", err)
	}
	target, err := fsStore.EnsureExecutionTarget(context.Background(), runtimefs.EnsureExecutionTargetRequest{
		RunID: "run_terminal_handoff", TargetMode: PublishModeDirect, SourceRoot: projectRoot, ExecutionRoot: fsStore.RunTargetRoot("run_terminal_handoff"), PublishRoot: projectRoot,
		SourceSnapshotID: baseline.SnapshotID, TrustedSnapshotID: baseline.SnapshotID, PublishedSnapshotID: baseline.SnapshotID,
		State: "trusted", CreatedAt: terminalHandoffFixtureNow, UpdatedAt: terminalHandoffFixtureNow,
	})
	if err != nil {
		t.Fatalf("create terminal handoff execution target: %v", err)
	}
	if err := fsStore.RestoreSnapshot(context.Background(), baseline.SnapshotID, target.ExecutionRoot); err != nil {
		t.Fatalf("restore terminal handoff execution target: %v", err)
	}
	schemaSet, err := schemas.Load(filepath.Join(repoRoot(t), "contracts", "schemas"))
	if err != nil {
		t.Fatalf("load schemas: %v", err)
	}
	store := executionstore.New(db)
	requests := newRequestFixture(db, nil)
	loop := &orchestrationLoop{
		db:             db,
		provider:       requests.provider,
		transition:     state.NewTransitionService(db, schemaSet),
		validator:      state.NewWorkerOutputValidator(db, schemaSet),
		executionStore: store,
		events:         state.NewAttemptEventIngestor(db, schemaSet),
		fsStore:        fsStore,
		schemas:        schemaSet,
		yoloRoot:       projectRoot,
		publishMode:    PublishModeDirect,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
		},
		sleep: func(time.Duration) {},
	}
	return terminalHandoffHarness{db: db, store: store, fsStore: fsStore, requests: requests, loop: loop, runtimeDir: runtimeDir, projectRoot: projectRoot, baselineSnapshotID: baselineSnapshotID}
}

func (h terminalHandoffHarness) addInvocation(t *testing.T, index int) executionstore.ProviderInvocation {
	t.Helper()
	nodeRunID := fmt.Sprintf("nr_terminal_handoff_%03d", index)
	attemptID := fmt.Sprintf("run_terminal_handoff:implementation:%d", index)
	invocationID := fmt.Sprintf("pinv_terminal_handoff_%03d", index)
	sessionID := fmt.Sprintf("session_terminal_handoff_%03d", index)
	owner := fmt.Sprintf("handoff-worker-%03d", index)
	mustExec(t, h.db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (%q, 'run_terminal_handoff', 'implementation', %d, 1, 'running', 'builder', 'agent_terminal_handoff', 1, %q, NULL)`, nodeRunID, index, terminalHandoffFixtureNow))
	if err := h.store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:             attemptID,
		RunID:                 "run_terminal_handoff",
		NodeRunID:             nodeRunID,
		ExecutionMode:         executionstore.ExecutionModeProviderBacked,
		CompletionMode:        executionstore.ProviderCompletionModeServerResponse,
		ProviderTimeoutMillis: 60000,
		SessionRef:            sessionID,
		CreatedAt:             terminalHandoffFixtureNow,
	}); err != nil {
		t.Fatalf("enqueue provider attempt %d: %v", index, err)
	}
	if _, err := h.db.Exec(`UPDATE attempt_executions SET base_snapshot_id = ? WHERE attempt_id = ?`, h.baselineSnapshotID, attemptID); err != nil {
		t.Fatalf("record provider attempt %d base snapshot: %v", index, err)
	}
	claim, acquired, err := h.store.ClaimQueuedProviderAttempt(context.Background(), executionstore.ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: owner,
		LeaseUntil: "2099-03-01T12:10:00Z",
		ClaimedAt:  terminalHandoffFixtureNow,
	})
	if err != nil || !acquired {
		t.Fatalf("claim provider attempt %d: claim=%#v acquired=%t err=%v", index, claim, acquired, err)
	}
	prepared := h.requests.prepare(t, attemptID, invocationID)
	invocation, err := h.store.ActivateProviderInvocation(context.Background(), executionstore.ActivateProviderInvocationRequest{
		RequestIdentity:      prepared.Identity,
		ProviderInvocationID: invocationID,
		AttemptID:            attemptID,
		RunID:                "run_terminal_handoff",
		NodeRunID:            nodeRunID,
		LogicalSessionID:     sessionID,
		ProviderSessionID:    prepared.ProviderSessionID,
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		IssuedAt:             terminalHandoffFixtureNow,
		CreatedAt:            terminalHandoffFixtureNow,
		OpenedAt:             terminalHandoffFixtureNow,
		ExpectedLeaseOwner:   claim.LeaseOwner,
		ExpectedLeaseVersion: claim.LeaseVersion,
	})
	if err != nil {
		t.Fatalf("activate provider invocation %d: %v", index, err)
	}
	mustExec(t, h.db, fmt.Sprintf(`UPDATE attempt_executions SET state = 'running', started_at = %q, updated_at = %q WHERE attempt_id = %q`, terminalHandoffFixtureNow, terminalHandoffFixtureNow, attemptID))
	return invocation
}

func (h terminalHandoffHarness) runningHandoffJob(t *testing.T, invocation executionstore.ProviderInvocation, index int) asyncExecutionResult {
	t.Helper()
	job := h.baseHandoffJob(invocation, index)
	mustExec(t, h.db, fmt.Sprintf(`UPDATE attempt_executions SET state = 'running', started_at = %q WHERE attempt_id = %q`, terminalHandoffFixtureNow, invocation.AttemptID))
	return job
}

// configureSuccessfulHandoff runs the job's prepared request to a successful
// answer, so the job carries the verified terminal that settlement requires.
func (h terminalHandoffHarness) configureSuccessfulHandoff(t *testing.T, job *asyncExecutionResult, output string) {
	t.Helper()
	h.requests.complete(t, job, output)
}

func persistProviderTerminalHandoffForTest(t *testing.T, loop *orchestrationLoop, job *asyncExecutionResult) {
	t.Helper()
	if loop == nil || job == nil {
		t.Fatal("orchestration loop and execution result are required")
	}
	if loop.executionStore == nil && loop.db != nil {
		loop.executionStore = executionstore.New(loop.db)
	}
	if loop.fsStore == nil {
		fsStore, err := runtimefs.NewStore(loop.db, t.TempDir())
		if err != nil {
			t.Fatalf("new runtime filesystem store: %v", err)
		}
		loop.fsStore = fsStore
	}
	if loop.now == nil {
		loop.now = time.Now
	}
	job.attempt.ExecutionMode = executionstore.ExecutionModeProviderBacked
	job.attempt.CompletionMode = executionstore.ProviderCompletionModeServerResponse
	job.completionMode = executionstore.ProviderCompletionModeServerResponse
	if err := loop.persistProviderTerminalHandoff(context.Background(), job); err != nil {
		t.Fatalf("persist provider terminal handoff: %v", err)
	}
}

func (h terminalHandoffHarness) persistHandoffJob(t *testing.T, invocation executionstore.ProviderInvocation, index int) asyncExecutionResult {
	t.Helper()
	job := h.runningHandoffJob(t, invocation, index)
	h.configureSuccessfulHandoff(t, &job, "Builder complete.")
	if err := h.loop.persistProviderTerminalHandoff(context.Background(), &job); err != nil {
		t.Fatalf("persist terminal handoff: %v", err)
	}
	return job
}

func TestTimeoutOwnedSettlementMarksHandoffPendingBeforeFallbackPublication(t *testing.T) {
	t.Parallel()
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	job := h.runningHandoffJob(t, invocation, 1)
	timeoutErr := errors.New("Provider execution exceeded its inactivity budget.")
	response := h.requests.run(t, job.attemptID, job.providerInvocationID, opencode.ExecuteResponse{
		Outcome:           opencode.ExecutionOutcome{State: "failed", ReasonCode: "timeout", FailureText: timeoutErr.Error(), EvidenceSource: "stream"},
		StreamObservation: opencode.InvocationStreamObservation{TimedOut: true},
	}, timeoutErr)
	applyExecuteResponseToAsyncResult(&job, response, timeoutErr)

	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 3, 0, time.UTC) }
	if err := h.loop.commitProviderExecutionCompletion(context.Background(), job); err != nil {
		t.Fatalf("commit timeout-owned provider completion: %v", err)
	}

	var pendingAt, recoveryAt sql.NullString
	if err := h.db.QueryRow(`SELECT terminal_handoff_pending_at, terminal_recovery_required_at FROM provider_invocations WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID).Scan(&pendingAt, &recoveryAt); err != nil {
		t.Fatalf("load timeout handoff markers: %v", err)
	}
	if !pendingAt.Valid || recoveryAt.Valid {
		t.Fatalf("timeout fallback handoff markers pending=%#v recovery=%#v", pendingAt, recoveryAt)
	}
	var settlementCount int
	if err := h.db.QueryRow(`SELECT COUNT(1) FROM provider_completion_settlements WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID).Scan(&settlementCount); err != nil || settlementCount != 1 {
		t.Fatalf("timeout-owned settlement rows=%d err=%v, want 1", settlementCount, err)
	}
	if _, found, err := h.fsStore.ReadProviderTerminalHandoff(invocation.ProviderInvocationID); err != nil || found {
		t.Fatalf("timeout-owned fallback left its consumed handoff file: found=%t err=%v", found, err)
	}
}

func TestRejectedProviderTerminalHandoffPendingDoesNotFenceActiveInvocation(t *testing.T) {
	t.Parallel()
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	job := h.runningHandoffJob(t, invocation, 1)
	job.expectedLeaseOwner = "stale-worker"

	err := h.loop.markProviderTerminalHandoffPending(context.Background(), job)
	if err == nil {
		t.Fatal("mark provider terminal handoff pending unexpectedly accepted stale lease owner")
	}
	if err := h.loop.markProviderTerminalRecoveryRequired(context.Background(), job); err == nil {
		t.Fatal("mark provider terminal recovery required unexpectedly accepted stale lease owner")
	}

	var pendingAt, recoveryAt sql.NullString
	if err := h.db.QueryRow(`SELECT terminal_handoff_pending_at, terminal_recovery_required_at FROM provider_invocations WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID).Scan(&pendingAt, &recoveryAt); err != nil {
		t.Fatalf("load rejected handoff markers: %v", err)
	}
	if pendingAt.Valid || recoveryAt.Valid {
		t.Fatalf("rejected handoff write fenced active invocation: pending=%#v recovery=%#v", pendingAt, recoveryAt)
	}
}

func TestTerminalHandoffReplaySerializesAgainstLostHandoffSettlement(t *testing.T) {
	t.Parallel()
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	job := h.runningHandoffJob(t, invocation, 1)
	h.configureSuccessfulHandoff(t, &job, "Builder complete.")
	if err := h.loop.markProviderTerminalHandoffPending(context.Background(), job); err != nil {
		t.Fatalf("mark handoff pending: %v", err)
	}
	persistHandoffFileOnlyFromJob(t, h, job, "2026-03-01T12:00:03Z")
	candidate, found, err := h.store.GetProviderInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found {
		t.Fatalf("load file replay candidate: found=%t err=%v", found, err)
	}

	lostJob := job
	lostJob.outcome = opencode.ExecutionOutcome{State: "failed", ReasonCode: "provider_terminal_handoff_lost", FailureText: "lost terminal handoff", EvidenceSource: "request"}
	lostJob.err = errors.New(lostJob.outcome.FailureText)
	unlock := h.loop.lockProviderTerminalHandoffFile(invocation.ProviderInvocationID)
	recoveryStarted := make(chan struct{})
	recoveryDone := make(chan error, 1)
	go func() {
		close(recoveryStarted)
		recoveryDone <- h.loop.recoverProviderTerminalHandoffFile(context.Background(), candidate)
	}()
	<-recoveryStarted
	lostStarted := make(chan struct{})
	lostDone := make(chan error, 1)
	go func() {
		close(lostStarted)
		lostDone <- h.loop.commitLostHandoffFailure(context.Background(), lostJob)
	}()
	<-lostStarted
	select {
	case err := <-recoveryDone:
		unlock()
		t.Fatalf("file replay bypassed the invocation lock: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	select {
	case err := <-lostDone:
		unlock()
		t.Fatalf("lost-handoff settlement bypassed the invocation lock: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	unlock()
	if err := <-recoveryDone; err != nil {
		t.Fatalf("recover file-backed handoff: %v", err)
	}
	if err := <-lostDone; err != nil {
		t.Fatalf("settle lost handoff after replay: %v", err)
	}
	if count := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_completion_settlements WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 0 {
		t.Fatalf("lost-handoff path settled before canonical handoff convergence: rows=%d", count)
	}
	if count := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_terminal_handoffs WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 1 {
		t.Fatalf("replayed durable handoff rows=%d, want 1", count)
	}

	if _, err := h.loop.convergeProviderTerminalHandoffsWithLimit(context.Background(), 8); err != nil {
		t.Fatalf("converge replayed handoff: %v", err)
	}
	settlement, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found || settlement.Outcome != executionstore.ProviderCompletionOutcomeSuccess {
		t.Fatalf("concurrent replay settlement found=%t settlement=%#v err=%v", found, settlement, err)
	}
	if _, found, err := h.fsStore.ReadProviderTerminalHandoff(invocation.ProviderInvocationID); err != nil || found {
		t.Fatalf("concurrent replay left an orphan handoff file: found=%t err=%v", found, err)
	}
	cleanupCandidates, err := h.store.ListProviderTerminalHandoffFileCleanupCandidates(context.Background(), "", 1)
	if err != nil || len(cleanupCandidates) != 0 {
		t.Fatalf("concurrent replay left cleanup debt: candidates=%#v err=%v", cleanupCandidates, err)
	}
}

func TestAttemptCancellationClosesAdmissionToFurtherCommandGuardians(t *testing.T) {
	t.Parallel()
	h := newTerminalHandoffHarness(t)
	h.loop.commandGuardianRuntimeRoot = h.runtimeDir
	h.addPendingGuardEvidence(t, 1)
	ctx := context.Background()
	invocation, found, err := h.store.GetProviderInvocation(ctx, "pinv_terminal_handoff_001")
	if err != nil || !found {
		t.Fatalf("load invocation: found=%t err=%v", found, err)
	}
	guards, err := h.store.ListPendingProviderCommandGuardians(ctx, "", 1)
	if err != nil || len(guards) != 1 {
		t.Fatalf("load original command guard: guards=%v err=%v", guards, err)
	}
	guard := guards[0]
	// A second command was admitted before cancellation but has not launched.
	h.requests.claimCommand(t, invocation.ProviderInvocationID)
	// Cancellation is queued in the same transaction that closes admission, so
	// the admitted command cannot gain a guardian afterwards.
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := providerrequest.CloseAttemptAdmissionTx(ctx, tx, invocation.AttemptID, "2026-03-01T12:00:05Z"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	_, err = h.store.AcknowledgeProviderCommandGuardian(ctx, executionstore.AcknowledgeProviderCommandGuardianRequest{
		GuardID: "guard_after_cancellation", ProviderInvocationID: invocation.ProviderInvocationID,
		AttemptID: invocation.AttemptID, RunID: invocation.RunID, NodeRunID: invocation.NodeRunID,
		CommandOrdinal: 2, Platform: guard.Platform, GuardianPID: 1002, GuardianIdentityToken: "guardian-after-cancel",
		ChildPID: 2002, ChildIdentityToken: "child-after-cancel", TreeID: "2002", TreeIdentityToken: "tree-after-cancel",
		QuiescenceCapability: "capability-after-cancel", ExpectedLeaseOwner: guard.LeaseOwner, ExpectedLeaseVersion: guard.LeaseVersion,
		StartedAt: "2026-03-01T12:10:00Z", AcknowledgedAt: "2026-03-01T12:10:00Z",
	})
	if err == nil || !strings.Contains(err.Error(), "admission is closed") {
		t.Fatalf("cancelled attempt acknowledged a subsequent command: %v", err)
	}
}

func TestGuardianRecoveryCancellationWaitsForInflightProviderWorkerAndNeverFabricatesClosure(t *testing.T) {
	t.Parallel()
	h := newTerminalHandoffHarness(t)
	h.loop.commandGuardianRuntimeRoot = h.runtimeDir
	h.addPendingGuardEvidence(t, 1)
	invocation, found, err := h.store.GetProviderInvocation(context.Background(), "pinv_terminal_handoff_001")
	if err != nil || !found {
		t.Fatalf("load guardian recovery invocation: found=%t err=%v", found, err)
	}
	const attemptID = "run_terminal_handoff:implementation:1"
	mustExec(t, h.db, `UPDATE node_runs SET node_status = 'blocked', finished_at = '2026-03-01T12:00:02Z' WHERE node_run_id = '`+invocation.NodeRunID+`'`)
	mustExec(t, h.db, `UPDATE attempt_executions SET state = 'committed_cancelled', committed_at = '2026-03-01T12:00:02Z', failure_code = 'agent_unresponsive' WHERE attempt_id = '`+attemptID+`'`)
	if err := h.store.QueueAttemptCancel(context.Background(), executionstore.QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_guardian_recovery_inflight",
		AttemptID:      attemptID,
		RunID:          invocation.RunID,
		NodeRunID:      invocation.NodeRunID,
		SessionID:      invocation.LogicalSessionID,
		ReasonCode:     "agent_unresponsive",
		ReasonText:     "provider did not stop",
		RequestedAt:    "2026-03-01T12:00:02Z",
		CreatedAt:      "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("queue cancellation handoff: %v", err)
	}
	const guardID = "guard_terminal_handoff_001"
	if err := os.Remove(providerGuardEvidencePath(h.runtimeDir, guardID)); err != nil {
		t.Fatalf("remove guardian evidence: %v", err)
	}
	h.loop.commandGuardianAbsent = func(executionstore.ProviderCommandGuardian) bool { return true }
	if result, err := h.loop.ingestPendingProviderCommandGuardiansPass(context.Background(), "", 1, true); err != nil || result.Visited != 1 || result.Blocked {
		t.Fatalf("quarantine absent guardian evidence: result=%#v err=%v", result, err)
	}
	if err := h.loop.transitionService().RequestAttemptStopRetry(context.Background(), state.AttemptStopRetryRequest{
		RunID:      invocation.RunID,
		AttemptID:  attemptID,
		ActorID:    "local-operator",
		RoleID:     "operator",
		OccurredAt: "2026-03-01T12:00:03Z",
		GenerateID: func(prefix string) string { return prefix + "_guardian_recovery" },
	}); err != nil {
		t.Fatalf("authorize guardian-evidence recovery: %v", err)
	}
	adapter := &timeoutCancelObserverAdapter{}
	h.requests.respondWith(adapter)
	h.loop.inflightExec = map[string]struct{}{attemptID: {}}
	h.loop.inflightRunIDs = map[string]string{attemptID: invocation.RunID}

	if _, err := h.loop.drainAttemptCancelOutbox(context.Background()); err != nil {
		t.Fatalf("drain cancellation with active provider worker: %v", err)
	}
	if got := adapter.cancelCalls(); got != 0 {
		t.Fatalf("guardian recovery unexpectedly sent an additional stop while worker was active: calls=%d", got)
	}
	var status, handoffState string
	if err := h.db.QueryRow(`SELECT status, handoff_state FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel_guardian_recovery_inflight'`).Scan(&status, &handoffState); err != nil {
		t.Fatalf("load pending guardian recovery handoff: %v", err)
	}
	if status != "pending" || handoffState != "guardian_evidence_recovery_requested" {
		t.Fatalf("in-flight guardian recovery released cancellation: status=%q handoff_state=%q", status, handoffState)
	}
	h.loop.clearExecutionInflight(attemptID)
	// Operator authorization cannot close the request: its admitted command has
	// no guardian proof, and the request is not disposed without it.
	if processed, err := h.loop.drainAttemptCancelOutbox(context.Background()); !errors.Is(err, providerrequest.ErrResourcesOpen) || processed != 0 {
		t.Fatalf("authorized recovery released a request with an unproven command: processed=%d err=%v", processed, err)
	}
	assertCancellationHandoff(t, h, attemptID, "pending", "guardian_evidence_recovery_requested")
	if quiescent, err := h.store.AttemptCommandGuardiansQuiescent(context.Background(), attemptID); err != nil || quiescent {
		t.Fatalf("operator recovery fabricated process proof: quiescent=%t err=%v", quiescent, err)
	}
	if got := adapter.cancelCalls(); got != 0 {
		t.Fatalf("authorized recovery repeated stop: calls=%d", got)
	}
}

func (h terminalHandoffHarness) persistHandoffFileOnly(t *testing.T, invocation executionstore.ProviderInvocation, index int, createdAt string) asyncExecutionResult {
	t.Helper()
	job := h.runningHandoffJob(t, invocation, index)
	h.configureSuccessfulHandoff(t, &job, "Builder complete.")
	if err := h.loop.markProviderTerminalHandoffPending(context.Background(), job); err != nil {
		t.Fatalf("mark handoff pending: %v", err)
	}
	persistHandoffFileOnlyFromJob(t, h, job, createdAt)
	return job
}

// persistHandoffFileOnlyFromJob publishes a job's trusted handoff file and
// content, then interrupts the SQLite insert, as a crash between them would.
func persistHandoffFileOnlyFromJob(t *testing.T, h terminalHandoffHarness, job asyncExecutionResult, createdAt string) {
	t.Helper()
	observedAt, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		t.Fatalf("parse handoff time: %v", err)
	}
	previous := h.loop.now
	h.loop.now = func() time.Time { return observedAt }
	defer func() { h.loop.now = previous }()
	mustExec(t, h.db, `CREATE TRIGGER interrupt_file_only_handoff BEFORE INSERT ON provider_terminal_handoffs BEGIN SELECT RAISE(ABORT,'simulated crash before DB handoff'); END`)
	defer mustExec(t, h.db, `DROP TRIGGER interrupt_file_only_handoff`)
	if err := h.loop.persistProviderTerminalHandoff(context.Background(), &job); err == nil {
		t.Fatal("expected the interrupted SQLite handoff to fail")
	}
}

func TestProviderTerminalHandoffFileCleanupReplayRemovesOrphanAndClearsDebt(t *testing.T) {
	t.Parallel()
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	h.persistHandoffFileOnly(t, invocation, 1, "2026-03-01T12:00:03Z")
	mustExec(t, h.db, `UPDATE provider_invocations SET invocation_state = 'settled_failed', settled_at = '2026-03-01T12:00:04Z' WHERE provider_invocation_id = '`+invocation.ProviderInvocationID+`'`)
	if err := h.store.MarkProviderTerminalHandoffFileCleanupPending(context.Background(), invocation.ProviderInvocationID, "2026-03-01T12:00:04Z"); err != nil {
		t.Fatalf("mark terminal handoff file cleanup pending: %v", err)
	}
	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 5, 0, time.UTC) }

	visited, err := h.loop.recoverProviderTerminalHandoffFileCleanupsWithLimit(context.Background(), 8)
	if err != nil || visited != 1 {
		t.Fatalf("recover handoff file cleanup: visited=%d err=%v", visited, err)
	}
	if _, found, readErr := h.fsStore.ReadProviderTerminalHandoff(invocation.ProviderInvocationID); readErr != nil || found {
		t.Fatalf("orphan handoff file remains: found=%t err=%v", found, readErr)
	}
	candidates, err := h.store.ListProviderTerminalHandoffFileCleanupCandidates(context.Background(), "", 1)
	if err != nil || len(candidates) != 0 {
		t.Fatalf("cleanup debt remains after exact file removal: candidates=%#v err=%v", candidates, err)
	}
}

func (h terminalHandoffHarness) baseHandoffJob(invocation executionstore.ProviderInvocation, index int) asyncExecutionResult {
	owner := fmt.Sprintf("handoff-worker-%03d", index)
	return asyncExecutionResult{
		run:                     runSnapshot{RunID: invocation.RunID, RunStatus: "running"},
		node:                    nodeDefinition{NodeID: "implementation", RequiredRole: "builder", NodeClass: "delegated_execution"},
		attempt:                 nodeAttempt{NodeRunID: invocation.NodeRunID, NodeID: "implementation", Attempt: index, RequiredRole: "builder", AgentID: "agent_terminal_handoff", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, LeaseOwner: owner, LeaseVersion: 1},
		attemptID:               invocation.AttemptID,
		providerInvocationID:    invocation.ProviderInvocationID,
		providerInvocationAt:    terminalHandoffFixtureNow,
		completionMode:          executionstore.ProviderCompletionModeServerResponse,
		expectedLeaseOwner:      owner,
		expectedLeaseVersion:    1,
		expectedLeaseExpiresAt:  "2026-03-01T12:10:00Z",
		providerLeaseDuration:   10 * time.Minute,
		workspaceID:             invocation.RunID,
		model:                   "test-model",
		occurredAt:              terminalHandoffFixtureNow,
		providerDiagnosticsJSON: `{}`,
	}
}

func TestProviderTerminalHandoffPublicationFailurePreventsSQLiteInsert(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	job := h.runningHandoffJob(t, invocation, 1)
	h.configureSuccessfulHandoff(t, &job, "Builder complete.")

	handoffsPath := filepath.Join(h.runtimeDir, "provider-completion", "handoffs")
	if err := os.MkdirAll(filepath.Dir(handoffsPath), 0o700); err != nil {
		t.Fatalf("create provider completion root: %v", err)
	}
	if err := os.WriteFile(handoffsPath, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("block handoff directory creation: %v", err)
	}
	if err := h.loop.persistProviderTerminalHandoff(context.Background(), &job); err == nil {
		t.Fatal("expected trusted handoff publication failure")
	}
	if count := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_terminal_handoffs WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 0 {
		t.Fatalf("failed file publication left %d SQLite handoff rows", count)
	}
}

func TestProviderTerminalHandoffFileBeforeSQLiteReplaySettlesAfterLeaseExpiry(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 4, 0, time.UTC) }
	invocation := h.addInvocation(t, 1)
	h.persistHandoffFileOnly(t, invocation, 1, "2026-03-01T12:00:04Z")
	mustExec(t, h.db, fmt.Sprintf(`UPDATE attempt_executions SET lease_expires_at = '2026-03-01T12:00:05Z' WHERE attempt_id = %q`, invocation.AttemptID))
	if count := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_terminal_handoffs WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 0 {
		t.Fatalf("file-only handoff created %d SQLite rows before replay", count)
	}
	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 10, 0, 0, time.UTC) }

	replayStartupHandoffsThroughMaintenance(t, h)
	settlement, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found || settlement.Outcome != executionstore.ProviderCompletionOutcomeSuccess {
		t.Fatalf("file-only handoff settlement found=%t settlement=%#v err=%v", found, settlement, err)
	}
	attempt, found, err := h.store.GetAttempt(context.Background(), invocation.AttemptID)
	if err != nil || !found || attempt.State != "committed_success" {
		t.Fatalf("file-only handoff attempt found=%t attempt=%#v err=%v", found, attempt, err)
	}
	if count := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_terminal_handoffs WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 0 {
		t.Fatalf("settled file-only handoff left %d SQLite rows", count)
	}
	if _, err := os.Stat(providerTerminalHandoffTestPath(h.runtimeDir, invocation.ProviderInvocationID)); !os.IsNotExist(err) {
		t.Fatalf("replayed handoff file remains: %v", err)
	}
}

func TestProviderTerminalHandoffFileRecoveryFencesLeaseLineageAndExpiry(t *testing.T) {
	for _, tc := range []struct {
		name   string
		update string
	}{
		{name: "owner changed", update: `UPDATE attempt_executions SET lease_owner = 'replacement-worker' WHERE attempt_id = 'run_terminal_handoff:implementation:1'`},
		{name: "version changed", update: `UPDATE attempt_executions SET lease_version = lease_version + 1 WHERE attempt_id = 'run_terminal_handoff:implementation:1'`},
		{name: "observation after durable expiry", update: `UPDATE attempt_executions SET lease_expires_at = '2026-03-01T12:00:03Z' WHERE attempt_id = 'run_terminal_handoff:implementation:1'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTerminalHandoffHarness(t)
			h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 4, 0, time.UTC) }
			invocation := h.addInvocation(t, 1)
			h.persistHandoffFileOnly(t, invocation, 1, "2026-03-01T12:00:04Z")
			mustExec(t, h.db, tc.update)
			h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 10, 0, 0, time.UTC) }

			if _, err := h.loop.recoverProviderTerminalHandoffFilesWithLimit(context.Background(), 4); err == nil {
				t.Fatal("expected lease-fenced file replay to fail")
			}
			if _, err := os.Stat(providerTerminalHandoffTestPath(h.runtimeDir, invocation.ProviderInvocationID)); err != nil {
				t.Fatalf("fenced handoff file was not retained: %v", err)
			}
			if count := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_terminal_handoffs WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 0 {
				t.Fatalf("fenced file replay persisted %d SQLite rows", count)
			}
			if _, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID); err != nil || found {
				t.Fatalf("fenced file replay created a settlement: found=%t err=%v", found, err)
			}
		})
	}
}

func TestProviderTerminalHandoffChangedFileReplayConflictsAndRetainsOriginal(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 2, 0, time.UTC) }
	invocation := h.addInvocation(t, 1)
	job := h.runningHandoffJob(t, invocation, 1)
	h.configureSuccessfulHandoff(t, &job, "Original terminal response.")
	mustExec(t, h.db, `CREATE TRIGGER fail_terminal_handoff_insert BEFORE INSERT ON provider_terminal_handoffs BEGIN SELECT RAISE(FAIL, 'forced handoff insert failure'); END`)
	if err := h.loop.persistProviderTerminalHandoff(context.Background(), &job); err == nil {
		t.Fatal("expected initial SQLite handoff failure")
	}
	handoffPath := providerTerminalHandoffTestPath(h.runtimeDir, invocation.ProviderInvocationID)
	original, err := os.ReadFile(handoffPath)
	if err != nil {
		t.Fatalf("read original trusted handoff: %v", err)
	}
	job.tokens.TotalTokens++
	if err := h.loop.persistProviderTerminalHandoff(context.Background(), &job); err == nil || !strings.Contains(err.Error(), "replay conflicts") {
		t.Fatalf("changed trusted handoff replay error=%v, want conflict", err)
	}
	after, err := os.ReadFile(handoffPath)
	if err != nil || string(after) != string(original) {
		t.Fatalf("changed replay replaced original handoff: err=%v", err)
	}
}

func TestProviderTerminalHandoffMalformedFileFailsClosed(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	job := h.runningHandoffJob(t, invocation, 1)
	h.configureSuccessfulHandoff(t, &job, "Builder complete.")
	if err := h.loop.markProviderTerminalHandoffPending(context.Background(), job); err != nil {
		t.Fatalf("mark handoff pending: %v", err)
	}
	if _, err := h.fsStore.PersistProviderTerminalHandoff(invocation.ProviderInvocationID, []byte(`{"version":"v1","unknown":true}`)); err != nil {
		t.Fatalf("persist malformed handoff fixture: %v", err)
	}

	runtime := &Runtime{orchestrator: h.loop, completionEvery: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer runtime.stopMaintenance()
	runtime.beginStartupRecovery()
	if err := runtime.recoverDurableExecutionState(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.loop.waitForMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	if caughtUp, err := runtime.replayPendingStartupRecovery(ctx); err != nil || caughtUp || runtime.pendingStartupRecoveryStage() != startupRecoveryStageHandoffs {
		t.Fatalf("malformed file released startup authority gate: caught_up=%t stage=%v err=%v", caughtUp, runtime.pendingStartupRecoveryStage(), err)
	}
	err := runtime.drainMaintenance(ctx)
	if waitErr := h.loop.waitForMaintenance(ctx); waitErr != nil {
		t.Fatal(waitErr)
	}
	var infrastructureErr *providerTerminalHandoffInfrastructureError
	if !errors.As(err, &infrastructureErr) {
		t.Fatalf("malformed handoff error=%v, want fail-closed infrastructure error", err)
	}
	if _, err := os.Stat(providerTerminalHandoffTestPath(h.runtimeDir, invocation.ProviderInvocationID)); err != nil {
		t.Fatalf("malformed trusted handoff was not retained: %v", err)
	}
	if count := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_terminal_handoffs WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 0 {
		t.Fatalf("malformed handoff entered SQLite: rows=%d", count)
	}
	if _, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID); err != nil || found {
		t.Fatalf("malformed handoff authorized settlement: found=%t err=%v", found, err)
	}
	attempt, found, err := h.store.GetAttempt(context.Background(), invocation.AttemptID)
	if err != nil || !found || attempt.State != "running" {
		t.Fatalf("malformed handoff changed attempt: found=%t attempt=%#v err=%v", found, attempt, err)
	}
}

func TestProviderTerminalHandoffFileCleanupOnlyForTerminalInvocation(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	if _, err := h.fsStore.PersistProviderTerminalHandoff(invocation.ProviderInvocationID, []byte("not-json")); err != nil {
		t.Fatalf("persist terminal cleanup fixture: %v", err)
	}
	mustExec(t, h.db, `UPDATE provider_invocations SET invocation_state = 'settled_failed', settled_at = '2026-03-01T12:00:03Z' WHERE provider_invocation_id = 'pinv_terminal_handoff_001'`)
	candidate, found, err := h.store.GetProviderInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found {
		t.Fatalf("load terminal invocation: found=%t err=%v", found, err)
	}
	if err := h.loop.recoverProviderTerminalHandoffFile(context.Background(), candidate); err != nil {
		t.Fatalf("clean terminal handoff file: %v", err)
	}
	if _, err := os.Stat(providerTerminalHandoffTestPath(h.runtimeDir, invocation.ProviderInvocationID)); !os.IsNotExist(err) {
		t.Fatalf("terminal handoff file remains: %v", err)
	}
	if count := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_terminal_handoffs WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 0 {
		t.Fatalf("terminal cleanup reopened handoff authority: rows=%d", count)
	}
}

func TestProviderTerminalHandoffWireDecoderRejectsUnknownAndTrailingFields(t *testing.T) {
	t.Parallel()
	valid, err := json.Marshal(providerTerminalHandoffWire{
		Version:              providerTerminalHandoffWireVersion,
		ProviderInvocationID: "invocation",
		AttemptID:            "attempt",
		RunID:                "run",
		NodeRunID:            "node-run",
		ExpectedLeaseOwner:   "worker",
		ExpectedLeaseVersion: 1,
		ExecutionPayloadJSON: `{}`,
		CreatedAt:            "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("marshal valid handoff wire: %v", err)
	}
	for _, raw := range [][]byte{
		[]byte(`{"version":"v1","unknown":true}`),
		append(append([]byte(nil), valid...), []byte(` {}`)...),
	} {
		if _, err := decodeProviderTerminalHandoffWire(raw); err == nil {
			t.Fatalf("expected strict handoff decode rejection for %s", raw)
		}
	}
}

func TestProviderTerminalHandoffSQLiteRowSurvivesFailedConvergence(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	job := h.persistHandoffJob(t, invocation, 1)
	mustExec(t, h.db, `CREATE TRIGGER fail_stream_evidence_insert BEFORE INSERT ON provider_completion_evidence BEGIN SELECT RAISE(FAIL, 'forced stream evidence failure'); END`)

	if err := h.loop.convergeProviderTerminalHandoff(context.Background(), mustProviderTerminalHandoff(t, h.store, job.terminalHandoffID)); err == nil {
		t.Fatal("expected terminal handoff convergence failure")
	}
	if _, found, err := h.store.GetProviderTerminalHandoff(context.Background(), job.terminalHandoffID); err != nil || !found {
		t.Fatalf("terminal handoff was not preserved after convergence failure: found=%t err=%v", found, err)
	}
	if count := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_completion_settlements WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 0 {
		t.Fatalf("failed convergence created %d settlements", count)
	}
	attempt, found, err := h.store.GetAttempt(context.Background(), invocation.AttemptID)
	if err != nil || !found || attempt.State != "running" {
		t.Fatalf("failed convergence changed attempt: found=%t attempt=%#v err=%v", found, attempt, err)
	}
}

func TestProviderTerminalHandoffSQLiteRowConvergesAfterLeaseExpiry(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 2, 0, time.UTC) }
	invocation := h.addInvocation(t, 1)
	job := h.persistHandoffJob(t, invocation, 1)
	mustExec(t, h.db, fmt.Sprintf(`UPDATE attempt_executions SET lease_expires_at = '2026-03-01T12:00:03Z' WHERE attempt_id = %q`, invocation.AttemptID))
	lateNow := func() time.Time { return time.Date(2026, 3, 1, 12, 10, 0, 0, time.UTC) }
	restarted := &orchestrationLoop{db: h.db, executionStore: h.store, fsStore: h.fsStore, schemas: h.loop.schemas, now: lateNow, sleep: func(time.Duration) {}}

	visited, err := restarted.convergeProviderTerminalHandoffs(context.Background())
	if err != nil || visited != 1 {
		t.Fatalf("converge persisted handoff after lease expiry: visited=%d err=%v", visited, err)
	}
	settlement, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found || settlement.Outcome != executionstore.ProviderCompletionOutcomeSuccess {
		t.Fatalf("persisted handoff settlement found=%t settlement=%#v err=%v", found, settlement, err)
	}
	if _, found, err := h.store.GetProviderTerminalHandoff(context.Background(), job.terminalHandoffID); err != nil || found {
		t.Fatalf("settled handoff row remains: found=%t err=%v", found, err)
	}
}

func TestStartupReplaysFileOnlyHandoffBeforeLostHandoffFailure(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 2, 0, time.UTC) }
	invocation := h.addInvocation(t, 1)
	h.persistHandoffFileOnly(t, invocation, 1, "2026-03-01T12:00:02Z")
	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 3, 0, time.UTC) }

	replayStartupHandoffsThroughMaintenance(t, h)
	settlement, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found {
		t.Fatalf("load startup handoff settlement: found=%t err=%v", found, err)
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeSuccess || settlement.FailureReasonCode != "" {
		t.Fatalf("file-only handoff was settled as lost instead of replayed: %#v", settlement)
	}
	if count := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_terminal_handoffs WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 0 {
		t.Fatalf("startup left %d handoff rows after file replay", count)
	}
}

func TestStartupHandoffAuthorityDoesNotWaitForSnapshotFinalization(t *testing.T) {
	for _, items := range []int{providerlimits.StartupReplayMaxItems, providerlimits.StartupReplayMaxItems + 1} {
		t.Run(fmt.Sprintf("items_%d", items), func(t *testing.T) {
			h := newTerminalHandoffHarness(t)
			for index := 1; index <= items; index++ {
				invocation := h.addInvocation(t, index)
				h.persistHandoffJob(t, invocation, index)
			}
			runtime := &Runtime{orchestrator: h.loop}
			unlock := h.loop.lockRunWorkspace("run_terminal_handoff")
			deferred, err := runtime.replayStartupProviderTerminalHandoffs(context.Background())
			admitted, admissionErr := h.loop.scheduleProviderTerminalFinalization(context.Background())
			unlock()
			if err != nil || deferred || admissionErr != nil || admitted != 0 {
				t.Fatalf("durable authority scan depended on filesystem finalization: deferred=%t admitted=%d err=%v admission_err=%v", deferred, admitted, err, admissionErr)
			}
			if settled := countTestRows(t, h.db, `SELECT COUNT(*) FROM provider_completion_settlements`); settled != 0 {
				t.Fatalf("startup synchronously settled %d handoffs", settled)
			}
			finalizePendingHandoffsThroughMaintenance(t, h)
			if settled := countTestRows(t, h.db, `SELECT COUNT(*) FROM provider_completion_settlements`); settled != items {
				t.Fatalf("bounded finalizer settled %d items, want %d", settled, items)
			}
		})
	}
}

func TestProviderTerminalHandoffMalformedSQLitePayloadRetainsCursor(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	first := h.addInvocation(t, 1)
	second := h.addInvocation(t, 2)
	firstJob := h.persistHandoffJob(t, first, 1)
	h.persistHandoffJob(t, second, 2)
	mustExec(t, h.db, fmt.Sprintf(`UPDATE provider_terminal_handoffs SET payload_json = '{' WHERE handoff_id = %q`, firstJob.terminalHandoffID))

	visited, err := h.loop.convergeProviderTerminalHandoffs(context.Background())
	if err == nil {
		t.Fatal("expected malformed durable handoff payload to fail closed")
	}
	if visited != 0 || h.loop.terminalHandoffCursor != "" {
		t.Fatalf("malformed handoff advanced cursor: visited=%d cursor=%q", visited, h.loop.terminalHandoffCursor)
	}
	if _, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), second.ProviderInvocationID); err != nil || found {
		t.Fatalf("malformed first handoff allowed later settlement: found=%t err=%v", found, err)
	}
}

func TestProviderTerminalHandoffPassVisitsAtMostOneHundredTwentyEight(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	for index := 1; index <= providerTerminalHandoffPassLimit+1; index++ {
		invocation := h.addInvocation(t, index)
		h.persistHandoffJob(t, invocation, index)
	}
	visited, err := h.loop.convergeProviderTerminalHandoffs(context.Background())
	if err != nil {
		t.Fatalf("converge bounded terminal handoff pass: %v", err)
	}
	settled := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_completion_settlements`)
	remaining := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_terminal_handoffs`)
	if visited != providerTerminalHandoffPassLimit || settled != providerTerminalHandoffPassLimit || remaining != 1 {
		t.Fatalf("terminal handoff pass exceeded bound: visited=%d settlements=%d handoffs=%d", visited, settled, remaining)
	}
}

func TestStartupReplayBoundsPendingProviderCommandGuardianIngestion(t *testing.T) {
	for _, tc := range []struct {
		name     string
		items    int
		deferred bool
	}{
		{name: "exact budget", items: providerlimits.StartupReplayMaxItems},
		{name: "one item overflow", items: providerlimits.StartupReplayMaxItems + 1, deferred: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTerminalHandoffHarness(t)
			h.loop.commandGuardianRuntimeRoot = h.runtimeDir
			for index := 1; index <= tc.items; index++ {
				h.addPendingGuardEvidence(t, index)
			}

			deferred, err := (&Runtime{orchestrator: h.loop}).replayStartupProviderCommandGuardians(context.Background())
			if err != nil {
				t.Fatalf("replay pending command guardians: %v", err)
			}
			if deferred != tc.deferred {
				t.Fatalf("deferred=%t, want %t", deferred, tc.deferred)
			}
			quiescent := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_process_guards WHERE lifecycle = 'quiescent'`)
			want := min(tc.items, providerlimits.StartupReplayMaxItems)
			if quiescent != want {
				t.Fatalf("startup ingested %d guards, want %d", quiescent, want)
			}
			if deferred {
				if err := h.loop.ingestPendingProviderCommandGuardians(context.Background()); err != nil {
					t.Fatalf("normal command guardian loop did not continue deferred work: %v", err)
				}
				quiescent = countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_process_guards WHERE lifecycle = 'quiescent'`)
				if quiescent != tc.items {
					t.Fatalf("normal command guardian loop ingested %d guards, want %d", quiescent, tc.items)
				}
			}
		})
	}
}

func TestReplayMarksCleanupCompleteForQuiescentProviderGuardWithMissingEvidence(t *testing.T) {
	ctx := context.Background()
	h := newTerminalHandoffHarness(t)
	h.loop.commandGuardianRuntimeRoot = h.runtimeDir
	h.addPendingGuardEvidence(t, 1)
	guardID, _, evidencePath := h.persistPendingProviderGuardQuiescence(t, 1)
	if err := os.Remove(evidencePath); err != nil {
		t.Fatalf("remove already-ingested guardian evidence: %v", err)
	}
	assertProviderGuardCleanupState(t, h, guardID, "quiescent", true)

	pass, err := h.loop.ingestPendingProviderCommandGuardiansPass(ctx, "", 1, false)
	if err != nil || pass.Visited != 1 || pass.NextCursor != guardID {
		t.Fatalf("replay missing quiescent evidence: pass=%#v err=%v", pass, err)
	}
	assertProviderGuardCleanupState(t, h, guardID, "quiescent", false)
}

func TestProviderCommandGuardianReplayRequiresDurableProofAndMatchingEvidence(t *testing.T) {
	for _, tc := range []struct {
		name        string
		quiescent   bool
		replacement bool
	}{
		{name: "acknowledged missing"},
		{name: "quiescent corrupt", quiescent: true},
		{name: "quiescent replaced", quiescent: true, replacement: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := newTerminalHandoffHarness(t)
			h.loop.commandGuardianRuntimeRoot = h.runtimeDir
			h.addPendingGuardEvidence(t, 1)
			const guardID = "guard_terminal_handoff_001"
			if tc.quiescent {
				_, _, evidencePath := h.persistPendingProviderGuardQuiescence(t, 1)
				payload := []byte(`{"type":"result","unexpected":true}`)
				if tc.replacement {
					replaced, err := json.Marshal(struct {
						Type   string                 `json:"type"`
						Result processguardian.Result `json:"result"`
					}{Type: "result", Result: processguardian.Result{Ownership: processguardian.Ownership{GuardID: guardID}, Quiescent: true, FailureText: "replaced evidence"}})
					if err != nil {
						t.Fatalf("marshal replacement evidence: %v", err)
					}
					payload = replaced
				}
				if err := os.WriteFile(evidencePath, payload, 0o600); err != nil {
					t.Fatalf("replace provider guard evidence: %v", err)
				}
			} else if err := os.Remove(providerGuardEvidencePath(h.runtimeDir, guardID)); err != nil {
				t.Fatalf("remove acknowledged guardian evidence: %v", err)
			}

			if _, err := h.loop.ingestPendingProviderCommandGuardiansPass(ctx, "", 1, false); err != nil {
				t.Fatalf("replay untrusted provider guard evidence: %v", err)
			}
			wantLifecycle := "acknowledged"
			if tc.quiescent {
				wantLifecycle = "quiescent"
			}
			assertProviderGuardCleanupState(t, h, guardID, wantLifecycle, true)
		})
	}
}

func TestProviderCommandGuardianCleanupAcceptsConcurrentEvidenceRemoval(t *testing.T) {
	ctx := context.Background()
	h := newTerminalHandoffHarness(t)
	h.loop.commandGuardianRuntimeRoot = h.runtimeDir
	h.addPendingGuardEvidence(t, 1)
	guardID, evidence, evidencePath := h.persistPendingProviderGuardQuiescence(t, 1)
	if err := os.Remove(evidencePath); err != nil {
		t.Fatalf("remove evidence after replay read: %v", err)
	}

	h.loop.removeAndMarkProviderCommandGuardianEvidenceCleaned(ctx, guardID, evidence)
	assertProviderGuardCleanupState(t, h, guardID, "quiescent", false)
}

func TestProviderCommandGuardianCleanupMarkerFailureIsRetryable(t *testing.T) {
	ctx := context.Background()
	h := newTerminalHandoffHarness(t)
	h.loop.commandGuardianRuntimeRoot = h.runtimeDir
	h.addPendingGuardEvidence(t, 1)
	guardID, _, evidencePath := h.persistPendingProviderGuardQuiescence(t, 1)
	if err := os.Remove(evidencePath); err != nil {
		t.Fatalf("remove already-ingested guardian evidence: %v", err)
	}
	mustExec(t, h.db, `CREATE TRIGGER fail_provider_guard_cleanup_marker BEFORE UPDATE OF evidence_cleaned_at ON provider_process_guards WHEN NEW.guard_id = 'guard_terminal_handoff_001' BEGIN SELECT RAISE(ABORT, 'cleanup marker unavailable'); END`)
	if _, err := h.loop.ingestPendingProviderCommandGuardiansPass(ctx, "", 1, false); err != nil {
		t.Fatalf("cleanup marker failure turned durable quiescence into replay failure: %v", err)
	}
	assertProviderGuardCleanupState(t, h, guardID, "quiescent", true)
	mustExec(t, h.db, `DROP TRIGGER fail_provider_guard_cleanup_marker`)
	if _, err := h.loop.ingestPendingProviderCommandGuardiansPass(ctx, "", 1, false); err != nil {
		t.Fatalf("retry provider guard cleanup marker: %v", err)
	}
	assertProviderGuardCleanupState(t, h, guardID, "quiescent", false)
}

func TestPendingGuardianEvidenceAbsenceQuarantinesCancelledAttemptWithoutProvingQuiescence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{name: "missing evidence", mutate: func(t *testing.T, path string) {
			t.Helper()
			if err := os.Remove(path); err != nil {
				t.Fatalf("remove guardian evidence: %v", err)
			}
		}},
		{name: "corrupt evidence", mutate: func(t *testing.T, path string) {
			t.Helper()
			if err := os.WriteFile(path, []byte(`{"type":"result","unexpected":true}`), 0o600); err != nil {
				t.Fatalf("corrupt guardian evidence: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTerminalHandoffHarness(t)
			h.loop.commandGuardianRuntimeRoot = h.runtimeDir
			h.addPendingGuardEvidence(t, 1)
			const attemptID = "run_terminal_handoff:implementation:1"
			mustExec(t, h.db, `UPDATE attempt_executions SET launch_started_at = '2026-03-01T12:00:01Z' WHERE attempt_id = 'run_terminal_handoff:implementation:1'`)
			const guardID = "guard_terminal_handoff_001"
			tc.mutate(t, providerGuardEvidencePath(h.runtimeDir, guardID))
			if err := h.store.QueueAttemptCancel(context.Background(), executionstore.QueueAttemptCancelRequest{
				CancelOutboxID: "cancel_guardian_evidence_lost", AttemptID: attemptID, RunID: "run_terminal_handoff", NodeRunID: "nr_terminal_handoff_001", ReasonCode: "agent_unresponsive", RequestedAt: "2026-03-01T12:00:02Z", CreatedAt: "2026-03-01T12:00:02Z",
			}); err != nil {
				t.Fatalf("queue cancelled attempt handoff: %v", err)
			}
			h.loop.commandGuardianAbsent = func(executionstore.ProviderCommandGuardian) bool { return true }

			result, err := h.loop.ingestPendingProviderCommandGuardiansPass(context.Background(), "", 1, true)
			if err != nil || result.Blocked || result.Visited != 1 {
				t.Fatalf("ingest absent guardian evidence: result=%#v err=%v", result, err)
			}
			var lifecycle string
			var quarantinedAt sql.NullString
			if err := h.db.QueryRow(`SELECT lifecycle, quiescence_evidence_lost_at FROM provider_process_guards WHERE guard_id = ?`, guardID).Scan(&lifecycle, &quarantinedAt); err != nil {
				t.Fatalf("load quarantined guard: %v", err)
			}
			if lifecycle != "acknowledged" || !quarantinedAt.Valid {
				t.Fatalf("missing guardian evidence authorized quiescence: lifecycle=%q quarantined_at=%#v", lifecycle, quarantinedAt)
			}
			quiescent, err := h.store.AttemptCommandGuardiansQuiescent(context.Background(), attemptID)
			if err != nil || quiescent {
				t.Fatalf("quarantined guardian evidence authorized attempt quiescence: quiescent=%t err=%v", quiescent, err)
			}
			var handoffState string
			if err := h.db.QueryRow(`SELECT handoff_state FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel_guardian_evidence_lost'`).Scan(&handoffState); err != nil {
				t.Fatalf("load cancellation quarantine: %v", err)
			}
			if handoffState != "guardian_evidence_lost" {
				t.Fatalf("expected durable guardian evidence quarantine, got %q", handoffState)
			}
		})
	}
}

func TestGuardRecoveryPassFailureRetainsFailedCursorForNormalAndDeferredStartup(t *testing.T) {
	type recoveryPass struct {
		name     string
		seed     func(*testing.T, terminalHandoffHarness)
		arm      func(*testing.T, terminalHandoffHarness)
		drop     func(*testing.T, terminalHandoffHarness)
		normal   func(context.Context, terminalHandoffHarness) (string, error)
		deferred func(*Runtime) startupRecoveryStage
		cursor   func(*Runtime, terminalHandoffHarness) string
		count    func(*testing.T, terminalHandoffHarness) int
	}

	guardArm := func(t *testing.T, h terminalHandoffHarness) {
		t.Helper()
		mustExec(t, h.db, `CREATE TRIGGER fail_recovery_guard BEFORE UPDATE ON provider_process_guards WHEN NEW.guard_id = 'guard_terminal_handoff_001' AND NEW.lifecycle = 'quiescent' BEGIN SELECT RAISE(FAIL, 'forced fail_recovery_guard failure'); END`)
	}
	guardDrop := func(t *testing.T, h terminalHandoffHarness) {
		t.Helper()
		mustExec(t, h.db, `DROP TRIGGER fail_recovery_guard`)
	}
	for _, tc := range []recoveryPass{
		{
			name: "provider guard",
			seed: func(t *testing.T, h terminalHandoffHarness) {
				h.loop.commandGuardianRuntimeRoot = h.runtimeDir
				h.addPendingGuardEvidence(t, 1)
				h.addPendingGuardEvidence(t, 2)
			},
			arm:  guardArm,
			drop: guardDrop,
			normal: func(ctx context.Context, h terminalHandoffHarness) (string, error) {
				pass, err := h.loop.ingestPendingProviderCommandGuardiansPass(ctx, "", providerCommandGuardianPassLimit, false)
				return pass.NextCursor, err
			},
			deferred: func(rt *Runtime) startupRecoveryStage {
				rt.setStartupRecoveryPending(true, true, true)
				return startupRecoveryStageGuards
			},
			cursor: func(rt *Runtime, _ terminalHandoffHarness) string { return rt.startupGuardCursor },
			count: func(t *testing.T, h terminalHandoffHarness) int {
				return countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_process_guards WHERE lifecycle = 'quiescent'`)
			},
		},
	} {
		t.Run(tc.name+" normal", func(t *testing.T) {
			h := newTerminalHandoffHarness(t)
			tc.seed(t, h)
			tc.arm(t, h)
			cursor, err := tc.normal(context.Background(), h)
			if err == nil {
				t.Fatal("expected injected recovery failure")
			}
			if cursor != "" || tc.count(t, h) != 0 {
				t.Fatalf("failed pass advanced cursor=%q or processed later items=%d", cursor, tc.count(t, h))
			}
			tc.drop(t, h)
			if _, err := tc.normal(context.Background(), h); err != nil {
				t.Fatalf("retry recovery pass: %v", err)
			}
			if got := tc.count(t, h); got != 2 {
				t.Fatalf("retry did not resume at failed ID: processed=%d, want 2", got)
			}
		})

		t.Run(tc.name+" deferred startup", func(t *testing.T) {
			h := newTerminalHandoffHarness(t)
			tc.seed(t, h)
			rt := &Runtime{orchestrator: h.loop}
			rt.beginStartupRecovery()
			stage := tc.deferred(rt)
			tc.arm(t, h)
			caughtUp, err := rt.replayPendingStartupRecovery(context.Background())
			if err == nil {
				t.Fatal("expected injected deferred startup recovery failure")
			}
			if caughtUp || rt.pendingStartupRecoveryStage() != stage || rt.startupRecoveryIsOpen() || tc.cursor(rt, h) != "" || tc.count(t, h) != 0 {
				t.Fatalf("failed startup pass bypassed its cursor or gate: caught_up=%t stage=%d open=%t cursor=%q processed=%d", caughtUp, rt.pendingStartupRecoveryStage(), rt.startupRecoveryIsOpen(), tc.cursor(rt, h), tc.count(t, h))
			}
			tc.drop(t, h)
			if _, err := rt.replayPendingStartupRecovery(context.Background()); err != nil {
				t.Fatalf("retry deferred startup recovery: %v", err)
			}
			if got := tc.count(t, h); got != 2 {
				t.Fatalf("deferred retry did not resume at failed ID: processed=%d, want 2", got)
			}
		})
	}
}

func (h terminalHandoffHarness) persistPendingProviderGuardQuiescence(t *testing.T, index int) (string, string, string) {
	t.Helper()
	guardID := fmt.Sprintf("guard_terminal_handoff_%03d", index)
	result, digest, evidence, err := processguardian.ReadQuiescenceEvidence(h.runtimeDir, guardID)
	if err != nil {
		t.Fatalf("read provider guard evidence before ingest: %v", err)
	}
	quiescentAt := "2026-03-01T12:00:04Z"
	_, err = h.store.IngestProviderCommandGuardianQuiescence(context.Background(), executionstore.IngestProviderCommandGuardianQuiescenceRequest{
		GuardID: guardID, ProviderInvocationID: fmt.Sprintf("pinv_terminal_handoff_%03d", index),
		AttemptID: fmt.Sprintf("run_terminal_handoff:implementation:%d", index), RunID: "run_terminal_handoff",
		NodeRunID: fmt.Sprintf("nr_terminal_handoff_%03d", index), Platform: result.Platform,
		GuardianPID: int64(result.GuardianPID), GuardianIdentityToken: result.GuardianIdentityToken,
		ChildPID: int64(result.ChildPID), ChildIdentityToken: result.ChildIdentityToken, TreeID: strconv.Itoa(result.TreeID),
		TreeIdentityToken: result.TreeIdentityToken, QuiescenceCapability: result.QuiescenceCapability,
		ExitedAt: quiescentAt, QuiescentAt: quiescentAt, ExitCode: int64(result.ExitCode), ExitSignal: int64(result.ExitSignal),
		TerminationForced: result.Forced, FailureCode: result.FailureCode, FailureText: result.FailureText,
		EvidenceSHA256: digest, EvidenceJSON: evidence,
	})
	if err != nil {
		t.Fatalf("ingest provider guard evidence: %v", err)
	}
	return guardID, evidence, providerGuardEvidencePath(h.runtimeDir, guardID)
}

func providerGuardEvidencePath(runtimeRoot, guardID string) string {
	name := sha256.Sum256([]byte(guardID))
	return filepath.Join(runtimeRoot, "provider-process-guards", "quiescence", fmt.Sprintf("%x.json", name[:]))
}

func assertProviderGuardCleanupState(t *testing.T, h terminalHandoffHarness, guardID, wantLifecycle string, wantPending bool) {
	t.Helper()
	var lifecycle string
	var cleanedAt sql.NullString
	if err := h.db.QueryRow(`SELECT lifecycle, evidence_cleaned_at FROM provider_process_guards WHERE guard_id = ?`, guardID).Scan(&lifecycle, &cleanedAt); err != nil {
		t.Fatalf("load provider guard cleanup state: %v", err)
	}
	if lifecycle != wantLifecycle {
		t.Fatalf("provider guard lifecycle=%q, want %q", lifecycle, wantLifecycle)
	}
	markerPending := !cleanedAt.Valid
	if markerPending != wantPending {
		t.Fatalf("unexpected provider guard cleanup state: lifecycle=%q want=%q marker_valid=%t pending=%t", lifecycle, wantLifecycle, cleanedAt.Valid, markerPending)
	}
	pending, err := h.store.ListPendingProviderCommandGuardians(context.Background(), "", 2)
	if err != nil {
		t.Fatalf("list provider guards after cleanup replay: %v", err)
	}
	found := false
	for _, guard := range pending {
		if guard.GuardID == guardID {
			found = true
			break
		}
	}
	if found != wantPending {
		t.Fatalf("provider guard pending=%t, want %t (rows=%#v)", found, wantPending, pending)
	}
}

func (h terminalHandoffHarness) addPendingGuardEvidence(t *testing.T, index int) {
	t.Helper()
	invocation := h.addInvocation(t, index)
	job := h.baseHandoffJob(invocation, index)
	h.requests.claimCommand(t, invocation.ProviderInvocationID)
	guardID := fmt.Sprintf("guard_terminal_handoff_%03d", index)
	ownership := processguardian.Ownership{
		GuardID: guardID, GuardianPID: index + 1000, GuardianIdentityToken: fmt.Sprintf("guardian-%03d", index),
		ChildPID: index + 2000, ChildIdentityToken: fmt.Sprintf("child-%03d", index), TreeID: index + 2000,
		TreeIdentityToken: fmt.Sprintf("tree-%03d", index), Platform: "unix_process_group", QuiescenceCapability: fmt.Sprintf("guardian-quiescence-%03d", index),
	}
	if _, err := h.store.AcknowledgeProviderCommandGuardian(context.Background(), executionstore.AcknowledgeProviderCommandGuardianRequest{
		GuardID: ownership.GuardID, ProviderInvocationID: invocation.ProviderInvocationID, AttemptID: invocation.AttemptID, RunID: invocation.RunID, NodeRunID: invocation.NodeRunID,
		CommandOrdinal: 1, Platform: ownership.Platform, GuardianPID: int64(ownership.GuardianPID), GuardianIdentityToken: ownership.GuardianIdentityToken,
		ChildPID: int64(ownership.ChildPID), ChildIdentityToken: ownership.ChildIdentityToken, TreeID: strconv.Itoa(ownership.TreeID), TreeIdentityToken: ownership.TreeIdentityToken,
		QuiescenceCapability: ownership.QuiescenceCapability, ExpectedLeaseOwner: job.expectedLeaseOwner, ExpectedLeaseVersion: job.expectedLeaseVersion,
		StartedAt: "2026-03-01T11:59:59Z", AcknowledgedAt: "2026-03-01T11:59:59Z",
	}); err != nil {
		t.Fatalf("acknowledge pending command guardian: %v", err)
	}
	evidence, err := json.Marshal(struct {
		Type   string                 `json:"type"`
		Result processguardian.Result `json:"result"`
	}{Type: "result", Result: processguardian.Result{Ownership: ownership, Quiescent: true}})
	if err != nil {
		t.Fatalf("marshal command guardian evidence: %v", err)
	}
	evidenceDir := filepath.Join(h.runtimeDir, "provider-process-guards", "quiescence")
	if err := os.MkdirAll(evidenceDir, 0o700); err != nil {
		t.Fatalf("create command guardian evidence directory: %v", err)
	}
	if err := os.WriteFile(providerGuardEvidencePath(h.runtimeDir, guardID), evidence, 0o600); err != nil {
		t.Fatalf("write command guardian evidence: %v", err)
	}
	observed, _, _, err := processguardian.ReadQuiescenceEvidence(h.runtimeDir, guardID)
	if err != nil {
		t.Fatalf("read command guardian evidence: %v", err)
	}
	if observed.GuardID != guardID || observed.ChildPID != ownership.ChildPID || observed.TreeID != ownership.TreeID || observed.Platform != ownership.Platform || !observed.Quiescent {
		t.Fatalf("command guardian evidence decoded incorrectly: %#v", observed)
	}
}

func mustProviderTerminalHandoff(t *testing.T, store *executionstore.Store, handoffID string) executionstore.ProviderTerminalHandoff {
	t.Helper()
	handoff, found, err := store.GetProviderTerminalHandoff(context.Background(), handoffID)
	if err != nil || !found {
		t.Fatalf("load provider terminal handoff: found=%t err=%v", found, err)
	}
	return handoff
}

func providerTerminalHandoffTestPath(runtimeRoot, invocationID string) string {
	sum := sha256.Sum256([]byte(invocationID))
	return filepath.Join(runtimeRoot, "provider-completion", "handoffs", hex.EncodeToString(sum[:])+".json")
}

func countTestRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatalf("count test rows: %v", err)
	}
	return count
}

// Exercise the production asynchronous authority scan before finalization, so
// tests cannot accidentally settle file-only evidence ahead of lost recovery.
func replayStartupHandoffsThroughMaintenance(t *testing.T, h terminalHandoffHarness) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	runtime := &Runtime{orchestrator: h.loop}
	runtime.beginStartupRecovery()
	if err := runtime.recoverDurableExecutionState(ctx); err != nil {
		t.Fatal(err)
	}
	for runtime.startupRecoveryPending() {
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		if err := h.loop.waitForMaintenance(ctx); err != nil {
			t.Fatal(err)
		}
		if caughtUp, err := runtime.replayPendingStartupRecovery(ctx); err != nil {
			t.Fatal(err)
		} else if caughtUp {
			break
		}
	}
	if err := h.loop.waitForMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	finalizePendingHandoffsThroughMaintenance(t, h)
}

func finalizePendingHandoffsThroughMaintenance(t *testing.T, h terminalHandoffHarness) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for {
		pending, err := h.store.ListPendingProviderTerminalHandoffs(ctx, "", 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) == 0 {
			return
		}
		if _, err := h.loop.scheduleProviderTerminalFinalization(ctx); err != nil {
			t.Fatal(err)
		}
		if err := h.loop.waitForMaintenance(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTerminalFinalizationFailureRetainsDurableRetryAfterStartupAuthority(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	first := h.addInvocation(t, 1)
	second := h.addInvocation(t, 2)
	h.persistHandoffJob(t, first, 1)
	h.persistHandoffJob(t, second, 2)
	mustExec(t, h.db, `CREATE TRIGGER fail_async_finalization BEFORE INSERT ON provider_completion_settlements WHEN NEW.provider_invocation_id = 'pinv_terminal_handoff_001' BEGIN SELECT RAISE(FAIL, 'forced finalization failure'); END`)
	runtime := &Runtime{orchestrator: h.loop}
	if deferred, err := runtime.replayStartupProviderTerminalHandoffs(context.Background()); err != nil || deferred {
		t.Fatalf("persisted authority was blocked by deferred finalization: deferred=%t err=%v", deferred, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := h.loop.scheduleProviderTerminalFinalization(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.loop.waitForMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	if _, found, err := h.store.GetProviderCompletionSettlementByInvocation(ctx, first.ProviderInvocationID); err != nil || found {
		t.Fatalf("failed finalizer settled first invocation: found=%t err=%v", found, err)
	}
	if pending := countTestRows(t, h.db, `SELECT COUNT(*) FROM provider_terminal_handoffs WHERE provider_invocation_id=?`, first.ProviderInvocationID); pending != 1 {
		t.Fatal("failed finalizer lost durable retry record")
	}
	mustExec(t, h.db, `DROP TRIGGER fail_async_finalization`)
	finalizePendingHandoffsThroughMaintenance(t, h)
	if settled := countTestRows(t, h.db, `SELECT COUNT(*) FROM provider_completion_settlements`); settled != 2 {
		t.Fatalf("finalization retry settled %d invocations", settled)
	}
}

// lostHandoffCandidate is an attempt whose worker ended and fenced its terminal
// handoff, but died before the handoff became durable.
func (h terminalHandoffHarness) lostHandoffCandidate(t *testing.T, index int, answer string) executionstore.ProviderInvocation {
	t.Helper()
	invocation := h.addInvocation(t, index)
	job := h.runningHandoffJob(t, invocation, index)
	if answer != "" {
		h.configureSuccessfulHandoff(t, &job, answer)
	} else {
		binding := h.requests.issueDispatch(t, invocation.ProviderInvocationID)
		if err := h.requests.authority.CloseAdmission(context.Background(), binding.Identity); err != nil {
			t.Fatalf("close admission: %v", err)
		}
	}
	if err := h.loop.markProviderTerminalHandoffPending(context.Background(), job); err != nil {
		t.Fatalf("mark handoff pending: %v", err)
	}
	return invocation
}

func TestLostHandoffOfARequestWithoutProviderAnswerIsSettledByAbandonment(t *testing.T) {
	t.Parallel()
	h := newTerminalHandoffHarness(t)
	invocation := h.lostHandoffCandidate(t, 1, "")

	if err := h.loop.settleLostProviderTerminalHandoffs(context.Background(), false); err != nil {
		t.Fatalf("settle lost handoff: %v", err)
	}
	settlement, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found || settlement.Outcome != executionstore.ProviderCompletionOutcomeFailed || settlement.FailureReasonCode != "provider_terminal_handoff_lost" {
		t.Fatalf("lost handoff was not settled as a failure: found=%t settlement=%#v err=%v", found, settlement, err)
	}
	if executions := h.requests.executions(invocation.ProviderInvocationID); executions != 0 {
		t.Fatalf("recovery redispatched the provider request %d times", executions)
	}
}

func TestLostHandoffOfACompletedAnswerIsSettledAsLostBecauseNothingRecordedIt(t *testing.T) {
	t.Parallel()
	h := newTerminalHandoffHarness(t)
	invocation := h.lostHandoffCandidate(t, 1, "Builder complete.")

	if err := h.loop.settleLostProviderTerminalHandoffs(context.Background(), false); err != nil {
		t.Fatalf("settle lost handoff: %v", err)
	}
	settlement, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found || settlement.Outcome != executionstore.ProviderCompletionOutcomeFailed || settlement.FailureReasonCode != "provider_terminal_handoff_lost" {
		t.Fatalf("a completed answer whose handoff never became durable must settle as lost: found=%t settlement=%#v err=%v", found, settlement, err)
	}
}

func TestOrphanedRequestWithAnUnreceiptedWriteStaysBlockedWithoutStoppingOtherRecoveries(t *testing.T) {
	t.Parallel()
	h := newTerminalHandoffHarness(t)
	blocked := h.addInvocation(t, 1)
	binding := h.requests.issueDispatch(t, blocked.ProviderInvocationID)
	if _, err := h.requests.authority.AdmitNative(context.Background(), providerrequest.NativeCall{
		Identity: binding.Identity, MessageID: "msg_fixture_write", CallID: "call_fixture_write", Tool: "write", Input: []byte(`{"path":"a.txt"}`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.store.QueueAttemptCancel(context.Background(), executionstore.QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_blocked", AttemptID: blocked.AttemptID, RunID: blocked.RunID, NodeRunID: blocked.NodeRunID,
		SessionID: "session_terminal_handoff_001", ReasonCode: "run_terminalized", RequestedAt: terminalHandoffFixtureNow,
	}); err != nil {
		t.Fatal(err)
	}
	recoverable := h.lostHandoffCandidate(t, 2, "")

	if err := h.loop.settleLostProviderTerminalHandoffs(context.Background(), false); err != nil {
		t.Fatalf("a blocked request must not abort recovery: %v", err)
	}
	if _, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), blocked.ProviderInvocationID); err != nil || found {
		t.Fatalf("a request with an unreceipted write was settled: found=%t err=%v", found, err)
	}
	if _, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), recoverable.ProviderInvocationID); err != nil || !found {
		t.Fatalf("an unrelated lost handoff was not recovered: found=%t err=%v", found, err)
	}
	// The operator is told why, once, however often recovery looks again.
	if err := h.loop.settleLostProviderTerminalHandoffs(context.Background(), false); err != nil {
		t.Fatalf("second recovery pass: %v", err)
	}
	if count := countTestRows(t, h.db, `SELECT COUNT(1) FROM events WHERE attempt_id = ? AND event_type = 'attempt.recovery_blocked'`, blocked.AttemptID); count != 1 {
		t.Fatalf("recovery_blocked events=%d, want exactly 1", count)
	}
}

func TestCancelledAttemptWhoseWorkerIsGoneIsSettledCancelledAfterRestart(t *testing.T) {
	t.Parallel()
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	h.requests.issueDispatch(t, invocation.ProviderInvocationID)
	if err := h.store.QueueAttemptCancel(context.Background(), executionstore.QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_orphan", AttemptID: invocation.AttemptID, RunID: invocation.RunID, NodeRunID: invocation.NodeRunID,
		SessionID: "session_terminal_handoff_001", ReasonCode: "run_terminalized", RequestedAt: terminalHandoffFixtureNow,
	}); err != nil {
		t.Fatalf("queue cancellation: %v", err)
	}
	if quiescent, err := h.store.AttemptCommandGuardiansQuiescent(context.Background(), invocation.AttemptID); err != nil || quiescent {
		t.Fatalf("a request nobody settled must not be quiescent: %t %v", quiescent, err)
	}

	if err := h.loop.settleLostProviderTerminalHandoffs(context.Background(), false); err != nil {
		t.Fatalf("settle orphaned cancellation: %v", err)
	}
	settlement, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found || settlement.Outcome != executionstore.ProviderCompletionOutcomeCancelled {
		t.Fatalf("orphaned cancellation was not settled as cancelled: found=%t settlement=%#v err=%v", found, settlement, err)
	}
	if quiescent, err := h.store.AttemptCommandGuardiansQuiescent(context.Background(), invocation.AttemptID); err != nil || !quiescent {
		t.Fatalf("a settled cancellation must release the attempt: %t %v", quiescent, err)
	}
}
