package orchestration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/schemas"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/processguardian"
	"kingdom/internal/providerlimits"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/runtimefs"
)

type receiptPublishingTestAdapter struct {
	opencode.Adapter
}

func (a *receiptPublishingTestAdapter) Execute(ctx context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	response, err := a.Adapter.Execute(ctx, req)
	if err != nil || (strings.TrimSpace(response.Outcome.State) != "" && strings.TrimSpace(response.Outcome.State) != "success") {
		return response, err
	}
	if writeErr := writeTestCompletionReceipt(req, response.OutputText, structuredPayloadJSONForTestOutput(response.OutputText)); writeErr != nil {
		return opencode.ExecuteResponse{}, writeErr
	}
	return response, nil
}

func writeTestCompletionReceipt(req opencode.ExecuteRequest, outputText, structuredPayloadJSON string) error {
	if req.CompletionMode != opencode.CompletionModeReceiptRequired || strings.TrimSpace(outputText) == "" {
		return nil
	}
	structuredPayloadJSON = firstNonEmptyString(strings.TrimSpace(structuredPayloadJSON), "{}")
	raw, err := json.Marshal(struct {
		ProtocolVersion      string          `json:"protocol_version"`
		AttemptID            string          `json:"attempt_id"`
		InvocationID         string          `json:"invocation_id"`
		CompletionCapability string          `json:"completion_capability"`
		FinalText            string          `json:"final_text"`
		StructuredPayload    json.RawMessage `json:"structured_payload"`
	}{
		ProtocolVersion:      opencode.CompletionProtocolVersion,
		AttemptID:            strings.TrimSpace(req.AttemptID),
		InvocationID:         strings.TrimSpace(req.InvocationID),
		CompletionCapability: strings.TrimSpace(req.CompletionCapability),
		FinalText:            strings.TrimSpace(outputText),
		StructuredPayload:    json.RawMessage(structuredPayloadJSON),
	})
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(req.CompletionReceiptDir, "completion.json"), raw, 0o600)
}

type receiptIngestionHarness struct {
	db         *sql.DB
	store      *executionstore.Store
	fsStore    *runtimefs.Store
	loop       *orchestrationLoop
	runtimeDir string
}

func newReceiptIngestionHarness(t *testing.T) receiptIngestionHarness {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db)
	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_receipt_ingestion', 'builder-berserker', 'busy', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_receipt_ingestion', 'Receipt ingestion', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_receipt_ingestion', 'receipt-ingestion', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_receipt_ingestion', 'wfdef_receipt_ingestion', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_receipt_ingestion', NULL, 'decree_receipt_ingestion', 'wfdef_receipt_ingestion', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	runtimeDir := t.TempDir()
	fsStore, err := runtimefs.NewStore(db, runtimeDir)
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	schemaSet, err := schemas.Load(filepath.Join(repoRoot(t), "contracts", "schemas"))
	if err != nil {
		t.Fatalf("load schemas: %v", err)
	}
	store := executionstore.New(db)
	loop := &orchestrationLoop{
		db:             db,
		executionStore: store,
		fsStore:        fsStore,
		schemas:        schemaSet,
		now: func() time.Time {
			parsed, _ := time.Parse(time.RFC3339, now)
			return parsed
		},
		sleep: func(time.Duration) {},
	}
	return receiptIngestionHarness{db: db, store: store, fsStore: fsStore, loop: loop, runtimeDir: runtimeDir}
}

func (h receiptIngestionHarness) addInvocation(t *testing.T, index int) (executionstore.ProviderInvocation, string, string) {
	t.Helper()
	now := "2026-03-01T12:00:00Z"
	nodeRunID := fmt.Sprintf("nr_receipt_ingestion_%03d", index)
	attemptID := fmt.Sprintf("run_receipt_ingestion:delegated_execution_berserker:%d", index)
	invocationID := fmt.Sprintf("pinv_receipt_ingestion_%03d", index)
	capability := fmt.Sprintf("capability-%03d", index)
	mustExec(t, h.db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (%q, 'run_receipt_ingestion', 'delegated_execution_berserker', %d, 1, 'running', 'builder-berserker', 'agent_receipt_ingestion', 1, %q, NULL)`, nodeRunID, index, now))
	if err := h.store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:             attemptID,
		RunID:                 "run_receipt_ingestion",
		NodeRunID:             nodeRunID,
		ExecutionMode:         executionstore.ExecutionModeProviderBacked,
		CompletionMode:        executionstore.ProviderCompletionModeReceiptRequired,
		ProviderTimeoutMillis: 60000,
		SessionRef:            fmt.Sprintf("session_%03d", index),
		CreatedAt:             now,
	}); err != nil {
		t.Fatalf("enqueue receipt attempt %d: %v", index, err)
	}
	invocation, err := h.store.ActivateProviderInvocation(context.Background(), executionstore.ActivateProviderInvocationRequest{
		ProviderInvocationID: invocationID,
		AttemptID:            attemptID,
		RunID:                "run_receipt_ingestion",
		NodeRunID:            nodeRunID,
		LogicalSessionID:     fmt.Sprintf("session_%03d", index),
		CompletionMode:       executionstore.ProviderCompletionModeReceiptRequired,
		CompletionCapability: capability,
		IssuedAt:             now,
		CreatedAt:            now,
		OpenedAt:             now,
	})
	if err != nil {
		t.Fatalf("activate receipt invocation %d: %v", index, err)
	}
	inbox, err := h.fsStore.EnsureProviderCompletionInbox(invocationID)
	if err != nil {
		t.Fatalf("ensure receipt inbox %d: %v", index, err)
	}
	return invocation, capability, inbox
}

func receiptWire(t *testing.T, attemptID, invocationID, capability, finalText, structuredPayloadJSON string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"protocol_version":      "v1",
		"attempt_id":            attemptID,
		"invocation_id":         invocationID,
		"completion_capability": capability,
		"final_text":            finalText,
		"structured_payload":    json.RawMessage(structuredPayloadJSON),
	})
	if err != nil {
		t.Fatalf("marshal receipt wire: %v", err)
	}
	return raw
}

func TestProviderReceiptIngestionRoutesByInboxAndDeletesAfterCommit(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	first, _, firstInbox := h.addInvocation(t, 1)
	second, secondCapability, _ := h.addInvocation(t, 2)
	receiptPath := filepath.Join(firstInbox, "cross-invocation.json")
	if err := os.WriteFile(receiptPath, receiptWire(t, second.AttemptID, second.ProviderInvocationID, secondCapability, "Builder complete.", `{}`), 0o600); err != nil {
		t.Fatalf("write cross-invocation receipt: %v", err)
	}

	if err := h.loop.ingestProviderCompletionReceiptInvocation(context.Background(), first.ProviderInvocationID); err != nil {
		t.Fatalf("ingest cross-invocation receipt: %v", err)
	}
	if _, err := os.Stat(receiptPath); !os.IsNotExist(err) {
		t.Fatalf("receipt must be deleted after durable disposition, stat err=%v", err)
	}
	firstAfter, _, err := h.store.GetProviderInvocation(context.Background(), first.ProviderInvocationID)
	if err != nil {
		t.Fatalf("get routed invocation: %v", err)
	}
	if firstAfter.InvocationState != executionstore.ProviderInvocationStateInvalidHandshake || firstAfter.InvalidReason != "attempt_lineage_mismatch" {
		t.Fatalf("inbox-selected invocation was not invalidated: %#v", firstAfter)
	}
	secondAfter, _, err := h.store.GetProviderInvocation(context.Background(), second.ProviderInvocationID)
	if err != nil {
		t.Fatalf("get claimed invocation: %v", err)
	}
	if secondAfter.InvocationState != executionstore.ProviderInvocationStateOpen || secondAfter.ReceiptSeenCount != 0 {
		t.Fatalf("payload-claimed invocation must not receive the receipt: %#v", secondAfter)
	}
}

func TestProviderReceiptIngestionMalformedEvidenceIsSafe(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	invocation, _, inbox := h.addInvocation(t, 1)
	secret := "raw-capability-must-not-persist"
	receiptPath := filepath.Join(inbox, "malformed.json")
	if err := os.WriteFile(receiptPath, []byte(`{"completion_capability":"`+secret+`","unexpected":"raw bytes"}`), 0o600); err != nil {
		t.Fatalf("write malformed receipt: %v", err)
	}

	if err := h.loop.ingestProviderCompletionReceiptInvocation(context.Background(), invocation.ProviderInvocationID); err != nil {
		t.Fatalf("ingest malformed receipt: %v", err)
	}
	evidence, err := h.store.ListProviderCompletionEvidence(context.Background(), invocation.ProviderInvocationID)
	if err != nil {
		t.Fatalf("list malformed evidence: %v", err)
	}
	if len(evidence) != 1 || evidence[0].EvidenceKind != executionstore.ProviderCompletionEvidenceKindCompletionReceiptAttempt {
		t.Fatalf("expected one receipt-attempt evidence row, got %#v", evidence)
	}
	if strings.Contains(evidence[0].PayloadJSON, secret) || strings.Contains(evidence[0].PayloadJSON, "raw bytes") || strings.Contains(evidence[0].PayloadJSON, "completion_capability_sha256") && strings.Contains(evidence[0].PayloadJSON, secret) {
		t.Fatalf("malformed evidence retained provider bytes or capability: %s", evidence[0].PayloadJSON)
	}
	if !strings.Contains(evidence[0].PayloadJSON, `"receipt_schema_validation_failed"`) {
		t.Fatalf("malformed evidence lacks safe disposition: %s", evidence[0].PayloadJSON)
	}
	if !strings.Contains(evidence[0].PayloadJSON, `"reason_text":"completion receipt does not match the required schema"`) {
		t.Fatalf("malformed evidence did not use the fixed safe reason text: %s", evidence[0].PayloadJSON)
	}
}

func TestProviderReceiptIngestionRetainsFileWhenSQLiteWriteFails(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	invocation, capability, inbox := h.addInvocation(t, 1)
	receiptPath := filepath.Join(inbox, "completion.json")
	if err := os.WriteFile(receiptPath, receiptWire(t, invocation.AttemptID, invocation.ProviderInvocationID, capability, "Builder complete.", `{}`), 0o600); err != nil {
		t.Fatalf("write completion receipt: %v", err)
	}
	mustExec(t, h.db, `CREATE TRIGGER fail_receipt_evidence BEFORE INSERT ON provider_completion_evidence BEGIN SELECT RAISE(FAIL, 'forced receipt SQL failure'); END`)

	if err := h.loop.ingestProviderCompletionReceiptInvocation(context.Background(), invocation.ProviderInvocationID); err == nil {
		t.Fatal("expected receipt ingestion SQL failure")
	}
	if _, err := os.Stat(receiptPath); err != nil {
		t.Fatalf("receipt must remain after SQL failure: %v", err)
	}
}

func TestProviderReceiptIngestionReplaysAcceptanceBeforeDeletion(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	invocation, capability, inbox := h.addInvocation(t, 1)
	wire := receiptWire(t, invocation.AttemptID, invocation.ProviderInvocationID, capability, "Builder complete.", `{}`)
	receiptPath := filepath.Join(inbox, "completion.json")
	if err := os.WriteFile(receiptPath, wire, 0o600); err != nil {
		t.Fatalf("write completion receipt: %v", err)
	}
	digest := sha256.Sum256(wire)
	if _, err := h.store.IngestProviderCompletionReceipt(context.Background(), executionstore.IngestProviderCompletionReceiptRequest{
		ProviderInvocationID:  invocation.ProviderInvocationID,
		EntryName:             "completion.json",
		WireByteCount:         int64(len(wire)),
		WireSHA256:            hex.EncodeToString(digest[:]),
		EntryType:             "regular",
		ProtocolVersion:       "v1",
		ObservedAttemptID:     invocation.AttemptID,
		ObservedInvocationID:  invocation.ProviderInvocationID,
		CompletionCapability:  capability,
		ValidationSucceeded:   true,
		FinalText:             "Builder complete.",
		StructuredPayloadJSON: `{}`,
		Disposition:           executionstore.ProviderCompletionReceiptDispositionAccepted,
		ObservedAt:            "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("persist receipt acceptance before simulated crash: %v", err)
	}
	if _, err := os.Stat(receiptPath); err != nil {
		t.Fatalf("simulated crash window lost receipt before cleanup replay: %v", err)
	}
	if err := h.loop.ingestProviderCompletionReceiptInvocation(context.Background(), invocation.ProviderInvocationID); err != nil {
		t.Fatalf("replay accepted receipt cleanup: %v", err)
	}
	if _, err := os.Stat(receiptPath); !os.IsNotExist(err) {
		t.Fatalf("accepted receipt was not deleted after idempotent replay: %v", err)
	}
	after, found, err := h.store.GetProviderInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found || after.ReceiptSeenCount != 1 {
		t.Fatalf("idempotent cleanup replay changed receipt counters: found=%t invocation=%#v err=%v", found, after, err)
	}
}

func TestProviderReceiptIngestionEntryOverflowInvalidatesAndProcessesOnlySixteen(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	invocation, _, inbox := h.addInvocation(t, 1)
	for index := 0; index < runtimefs.CompletionReceiptEntryLimit+1; index++ {
		path := filepath.Join(inbox, fmt.Sprintf("%02d.json", index))
		if err := os.WriteFile(path, []byte(`{}`), 0o600); err != nil {
			t.Fatalf("write overflow entry %d: %v", index, err)
		}
	}

	if err := h.loop.ingestProviderCompletionReceiptInvocation(context.Background(), invocation.ProviderInvocationID); err != nil {
		t.Fatalf("ingest overflowing inbox: %v", err)
	}
	entries, err := os.ReadDir(inbox)
	if err != nil {
		t.Fatalf("read overflowing inbox after pass: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("one pass must process only %d entries, remaining=%d", runtimefs.CompletionReceiptEntryLimit, len(entries))
	}
	after, _, err := h.store.GetProviderInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil {
		t.Fatalf("get overflow invocation: %v", err)
	}
	if after.InvocationState != executionstore.ProviderInvocationStateInvalidHandshake || after.InvalidReason != "receipt_entry_count_overflow" {
		t.Fatalf("entry overflow must invalidate active invocation: %#v", after)
	}
	evidence, err := h.store.ListProviderCompletionEvidence(context.Background(), invocation.ProviderInvocationID)
	if err != nil {
		t.Fatalf("list overflow evidence: %v", err)
	}
	if len(evidence) != runtimefs.CompletionReceiptEntryLimit+1 {
		t.Fatalf("expected overflow plus %d bounded dispositions, got %d", runtimefs.CompletionReceiptEntryLimit, len(evidence))
	}
}

func TestProviderReceiptIngestionPassVisitsAtMostOneHundredTwentyEightInboxes(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	for index := 1; index <= providerReceiptInvocationPassLimit+1; index++ {
		_, _, inbox := h.addInvocation(t, index)
		if err := os.WriteFile(filepath.Join(inbox, "malformed.json"), []byte(`{}`), 0o600); err != nil {
			t.Fatalf("write candidate receipt %d: %v", index, err)
		}
	}

	if _, err := h.loop.ingestProviderCompletionReceiptPass(context.Background()); err != nil {
		t.Fatalf("ingest candidate pass: %v", err)
	}
	var evidenceCount int
	if err := h.db.QueryRow(`SELECT COUNT(1) FROM provider_completion_evidence WHERE evidence_kind = 'completion_receipt_attempt'`).Scan(&evidenceCount); err != nil {
		t.Fatalf("count receipt attempt evidence: %v", err)
	}
	if evidenceCount != providerReceiptInvocationPassLimit {
		t.Fatalf("receipt pass processed %d inboxes, want %d", evidenceCount, providerReceiptInvocationPassLimit)
	}
	remaining := 0
	for index := 1; index <= providerReceiptInvocationPassLimit+1; index++ {
		invocationID := fmt.Sprintf("pinv_receipt_ingestion_%03d", index)
		entries, err := h.fsStore.ListProviderCompletionEntries(invocationID)
		if err != nil {
			t.Fatalf("list receipt candidate %d: %v", index, err)
		}
		remaining += len(entries)
	}
	if remaining != 1 {
		t.Fatalf("expected one unvisited inbox after bounded pass, got %d", remaining)
	}
}

func TestStartupReceiptReplayPersistsEvidenceWithoutSettling(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	invocation, capability, inbox := h.addInvocation(t, 1)
	if err := os.WriteFile(filepath.Join(inbox, "completion.json"), receiptWire(t, invocation.AttemptID, invocation.ProviderInvocationID, capability, "Builder complete.", `{}`), 0o600); err != nil {
		t.Fatalf("write startup receipt: %v", err)
	}
	runtime := &Runtime{orchestrator: h.loop}
	if err := runtime.recoverDurableExecutionState(context.Background()); err != nil {
		t.Fatalf("recover durable execution state: %v", err)
	}
	if _, found, err := h.store.GetProviderCompletionReceiptEvidence(context.Background(), invocation.ProviderInvocationID); err != nil || !found {
		t.Fatalf("startup did not replay receipt evidence: found=%v err=%v", found, err)
	}
	if _, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID); err != nil || found {
		t.Fatalf("receipt-only startup evidence must remain unsettled before guardian: found=%v err=%v", found, err)
	}
	attempt, found, err := h.store.GetAttempt(context.Background(), invocation.AttemptID)
	if err != nil || !found {
		t.Fatalf("get startup attempt: found=%v err=%v", found, err)
	}
	if attempt.State != "queued" {
		t.Fatalf("receipt replay inferred process success, attempt state=%q", attempt.State)
	}
}

func TestProviderTerminalHandoffIsPersistedBeforeReceiptIngestion(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	invocation, capability, inbox := h.addInvocation(t, 1)
	job := h.runningHandoffJob(t, invocation, capability, inbox, 1)
	h.loop.adapter = &sequenceExecutionAdapter{outputs: []string{"Builder complete."}}
	mustExec(t, h.db, `CREATE TRIGGER require_terminal_handoff_before_receipt
		BEFORE INSERT ON provider_completion_evidence
		WHEN NEW.evidence_kind = 'completion_receipt_attempt'
		 AND NOT EXISTS (SELECT 1 FROM provider_terminal_handoffs WHERE provider_invocation_id = NEW.provider_invocation_id)
		BEGIN SELECT RAISE(FAIL, 'terminal handoff must precede receipt ingestion'); END`)

	h.loop.startAsyncExecution(context.Background(), job)
	if !waitForProviderSettlement(h.store, invocation.ProviderInvocationID) {
		var handoffCount int
		_ = h.db.QueryRow(`SELECT COUNT(1) FROM provider_terminal_handoffs WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID).Scan(&handoffCount)
		t.Fatalf("timed out waiting for provider settlement %q; handoffs=%d", invocation.ProviderInvocationID, handoffCount)
	}
	attempt, found, err := h.store.GetAttempt(context.Background(), invocation.AttemptID)
	if err != nil || !found || attempt.State != "committed_success" {
		t.Fatalf("receipt-backed execution did not settle successfully: found=%t attempt=%#v err=%v", found, attempt, err)
	}
}

func TestProviderTerminalHandoffFileRecoversAfterInsertFailureAndLeaseExpiry(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 2, 0, time.UTC) }
	invocation, capability, inbox := h.addInvocation(t, 1)
	job := h.runningHandoffJob(t, invocation, capability, inbox, 1)
	zero := 0
	job.providerStreamObservation = opencode.InvocationStreamObservation{ProcessExited: true, ProcessExitCode: &zero, StdoutDrained: true, StderrDrained: true}
	job.outputText = "provider process output"
	job.outcome = opencode.ExecutionOutcome{State: "success", ReasonCode: "execution_completed"}
	receiptPath := filepath.Join(inbox, "completion.json")
	if err := os.WriteFile(receiptPath, receiptWire(t, invocation.AttemptID, invocation.ProviderInvocationID, capability, "Builder complete.", `{}`), 0o600); err != nil {
		t.Fatalf("write completion receipt: %v", err)
	}
	mustExec(t, h.db, `CREATE TRIGGER fail_terminal_handoff_insert BEFORE INSERT ON provider_terminal_handoffs BEGIN SELECT RAISE(FAIL, 'forced handoff insert failure'); END`)

	if err := h.loop.persistProviderTerminalHandoff(context.Background(), &job); err == nil || !strings.Contains(err.Error(), "forced handoff insert failure") {
		t.Fatalf("expected forced handoff insert failure, got %v", err)
	}
	handoffPath := providerTerminalHandoffTestPath(h.runtimeDir, invocation.ProviderInvocationID)
	if _, err := os.Stat(handoffPath); err != nil {
		t.Fatalf("trusted handoff file did not survive SQLite failure: %v", err)
	}
	if count := countReceiptRows(t, h.db, `SELECT COUNT(1) FROM provider_terminal_handoffs WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 0 {
		t.Fatalf("failed handoff insert left %d rows", count)
	}
	mustExec(t, h.db, `DROP TRIGGER fail_terminal_handoff_insert`)
	mustExec(t, h.db, `UPDATE attempt_executions SET lease_expires_at = '2026-03-01T12:00:03Z' WHERE attempt_id = 'run_receipt_ingestion:delegated_execution_berserker:1'`)

	restarted := &orchestrationLoop{db: h.db, executionStore: h.store, fsStore: h.fsStore, schemas: h.loop.schemas, now: func() time.Time { return time.Date(2026, 3, 1, 12, 10, 0, 0, time.UTC) }, sleep: func(time.Duration) {}}
	if err := (&Runtime{orchestrator: restarted}).recoverDurableExecutionState(context.Background()); err != nil {
		t.Fatalf("recover durable execution state after expired lease: %v", err)
	}
	settlement, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found || settlement.Outcome != executionstore.ProviderCompletionOutcomeSuccess {
		t.Fatalf("recovered settlement found=%t settlement=%#v err=%v", found, settlement, err)
	}
	attempt, found, err := h.store.GetAttempt(context.Background(), invocation.AttemptID)
	if err != nil || !found || attempt.State != "committed_success" {
		t.Fatalf("recovered attempt found=%t attempt=%#v err=%v", found, attempt, err)
	}
	if _, err := os.Stat(handoffPath); !os.IsNotExist(err) {
		t.Fatalf("recovered handoff file remains: %v", err)
	}
	if _, err := os.Stat(receiptPath); !os.IsNotExist(err) {
		t.Fatalf("accepted receipt file remains: %v", err)
	}
	if count := countReceiptRows(t, h.db, `SELECT COUNT(1) FROM provider_terminal_handoffs WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 0 {
		t.Fatalf("settlement left %d SQLite handoff rows", count)
	}
}

func TestProviderTerminalHandoffFileChangedReplayConflictsAndRetainsOriginal(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 2, 0, time.UTC) }
	invocation, capability, inbox := h.addInvocation(t, 1)
	job := h.runningHandoffJob(t, invocation, capability, inbox, 1)
	zero := 0
	job.providerStreamObservation = opencode.InvocationStreamObservation{ProcessExited: true, ProcessExitCode: &zero, StdoutDrained: true, StderrDrained: true}
	job.outputText = "original process output"
	job.outcome = opencode.ExecutionOutcome{State: "success", ReasonCode: "execution_completed"}
	mustExec(t, h.db, `CREATE TRIGGER fail_changed_replay_insert BEFORE INSERT ON provider_terminal_handoffs BEGIN SELECT RAISE(FAIL, 'forced handoff insert failure'); END`)
	if err := h.loop.persistProviderTerminalHandoff(context.Background(), &job); err == nil {
		t.Fatal("expected initial SQLite handoff failure")
	}
	handoffPath := providerTerminalHandoffTestPath(h.runtimeDir, invocation.ProviderInvocationID)
	original, err := os.ReadFile(handoffPath)
	if err != nil {
		t.Fatalf("read original trusted handoff: %v", err)
	}
	job.outputText = "changed process output"
	if err := h.loop.persistProviderTerminalHandoff(context.Background(), &job); err == nil || !strings.Contains(err.Error(), "replay conflicts") {
		t.Fatalf("expected changed trusted handoff replay conflict, got %v", err)
	}
	after, err := os.ReadFile(handoffPath)
	if err != nil || string(after) != string(original) {
		t.Fatalf("changed replay replaced original handoff: err=%v", err)
	}
}

func TestProviderTerminalHandoffFileMalformedWireIsRetainedAndBlocksReceipt(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	invocation, capability, inbox := h.addInvocation(t, 1)
	if _, err := h.fsStore.PersistProviderTerminalHandoff(invocation.ProviderInvocationID, []byte(`{"version":"v1","unknown":true}`)); err != nil {
		t.Fatalf("persist malformed trusted handoff fixture: %v", err)
	}
	receiptPath := filepath.Join(inbox, "completion.json")
	if err := os.WriteFile(receiptPath, receiptWire(t, invocation.AttemptID, invocation.ProviderInvocationID, capability, "Builder complete.", `{}`), 0o600); err != nil {
		t.Fatalf("write completion receipt: %v", err)
	}

	err := h.loop.ingestProviderCompletionReceiptInvocation(context.Background(), invocation.ProviderInvocationID)
	var infrastructureErr *providerReceiptInfrastructureError
	if !errors.As(err, &infrastructureErr) {
		t.Fatalf("malformed trusted handoff error = %v, want infrastructure error", err)
	}
	if _, err := os.Stat(providerTerminalHandoffTestPath(h.runtimeDir, invocation.ProviderInvocationID)); err != nil {
		t.Fatalf("malformed trusted handoff was not retained: %v", err)
	}
	if _, err := os.Stat(receiptPath); err != nil {
		t.Fatalf("receipt was consumed despite malformed trusted handoff: %v", err)
	}
	if _, found, err := h.store.GetProviderCompletionReceiptEvidence(context.Background(), invocation.ProviderInvocationID); err != nil || found {
		t.Fatalf("malformed trusted handoff allowed receipt evidence: found=%t err=%v", found, err)
	}
	if count := countReceiptRows(t, h.db, `SELECT COUNT(1) FROM provider_terminal_handoffs WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 0 {
		t.Fatalf("malformed trusted handoff entered SQLite: rows=%d", count)
	}
}

func TestProviderTerminalHandoffFileIsCleanupOnlyForTerminalInvocation(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	invocation, _, _ := h.addInvocation(t, 1)
	if _, err := h.fsStore.PersistProviderTerminalHandoff(invocation.ProviderInvocationID, []byte(`not-json`)); err != nil {
		t.Fatalf("persist terminal cleanup fixture: %v", err)
	}
	mustExec(t, h.db, `UPDATE provider_invocations SET invocation_state = 'settled_failed', settled_at = '2026-03-01T12:00:03Z' WHERE provider_invocation_id = 'pinv_receipt_ingestion_001'`)
	candidate, found, err := h.store.GetProviderInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found {
		t.Fatalf("load terminal invocation: found=%t err=%v", found, err)
	}

	if err := h.loop.ingestProviderCompletionReceiptCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("clean terminal handoff file: %v", err)
	}
	if _, err := os.Stat(providerTerminalHandoffTestPath(h.runtimeDir, invocation.ProviderInvocationID)); !os.IsNotExist(err) {
		t.Fatalf("terminal handoff file remains: %v", err)
	}
	if count := countReceiptRows(t, h.db, `SELECT COUNT(1) FROM provider_terminal_handoffs WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 0 {
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

func TestProviderTerminalHandoffPreservesReceiptWhenIngestionSQLFails(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	invocation, capability, inbox := h.addInvocation(t, 1)
	job := h.persistHandoffJob(t, invocation, 1)
	receiptPath := filepath.Join(inbox, "completion.json")
	if err := os.WriteFile(receiptPath, receiptWire(t, invocation.AttemptID, invocation.ProviderInvocationID, capability, "Builder complete.", `{}`), 0o600); err != nil {
		t.Fatalf("write completion receipt: %v", err)
	}
	mustExec(t, h.db, `CREATE TRIGGER fail_handoff_receipt_ingestion BEFORE INSERT ON provider_completion_evidence BEGIN SELECT RAISE(FAIL, 'forced ingestion failure'); END`)

	if err := h.loop.convergeProviderTerminalHandoff(context.Background(), mustProviderTerminalHandoff(t, h.store, job.terminalHandoffID)); err == nil {
		t.Fatal("expected terminal handoff convergence ingestion failure")
	}
	if _, found, err := h.store.GetProviderTerminalHandoff(context.Background(), job.terminalHandoffID); err != nil || !found {
		t.Fatalf("terminal handoff was not preserved after ingestion failure: found=%t err=%v", found, err)
	}
	if _, err := os.Stat(receiptPath); err != nil {
		t.Fatalf("receipt file was not preserved after ingestion failure: %v", err)
	}
	if _, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID); err != nil || found {
		t.Fatalf("ingestion failure must not settle provider execution: found=%t err=%v", found, err)
	}
}

func TestProviderTerminalHandoffRestartConvergesAcceptedReceiptAfterLeaseExpiry(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	invocation, capability, inbox := h.addInvocation(t, 1)
	job := h.persistHandoffJob(t, invocation, 1)
	if err := os.WriteFile(filepath.Join(inbox, "completion.json"), receiptWire(t, invocation.AttemptID, invocation.ProviderInvocationID, capability, "Builder complete.", `{}`), 0o600); err != nil {
		t.Fatalf("write completion receipt: %v", err)
	}
	if err := h.loop.ingestProviderCompletionReceiptInvocation(context.Background(), invocation.ProviderInvocationID); err != nil {
		t.Fatalf("persist completion receipt before restart: %v", err)
	}
	if _, err := h.db.Exec(`UPDATE attempt_executions SET lease_expires_at = '2000-01-01T00:00:00Z' WHERE attempt_id = ?`, invocation.AttemptID); err != nil {
		t.Fatalf("expire provider lease: %v", err)
	}

	restarted := &orchestrationLoop{db: h.db, executionStore: h.store, fsStore: h.fsStore, schemas: h.loop.schemas, now: h.loop.now, sleep: func(time.Duration) {}}
	if _, err := restarted.convergeProviderTerminalHandoffs(context.Background()); err != nil {
		t.Fatalf("converge terminal handoff after restart: %v", err)
	}
	settlement, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found || settlement.Outcome != executionstore.ProviderCompletionOutcomeSuccess {
		t.Fatalf("expired-lease handoff did not settle from accepted receipt: found=%t settlement=%#v err=%v", found, settlement, err)
	}
	if _, found, err := h.store.GetProviderTerminalHandoff(context.Background(), job.terminalHandoffID); err != nil || found {
		t.Fatalf("settled terminal handoff was not consumed atomically: found=%t err=%v", found, err)
	}
}

func TestProviderTerminalHandoffProcessExitWithoutReceiptSettlesFailure(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	invocation, _, _ := h.addInvocation(t, 1)
	h.persistHandoffJob(t, invocation, 1)
	if _, err := h.loop.convergeProviderTerminalHandoffs(context.Background()); err != nil {
		t.Fatalf("converge receipt-less terminal handoff: %v", err)
	}
	settlement, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found {
		t.Fatalf("load receipt-less settlement: found=%t err=%v", found, err)
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeFailed || settlement.FailureReasonCode != "provider_incomplete_turn" {
		t.Fatalf("process exit alone authorized success: %#v", settlement)
	}
}

func TestProviderTerminalHandoffConflictingReceiptSettlesFailure(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	invocation, capability, inbox := h.addInvocation(t, 1)
	h.persistHandoffJob(t, invocation, 1)
	for index, finalText := range []string{"Builder complete.", "Conflicting builder result."} {
		path := filepath.Join(inbox, fmt.Sprintf("completion-%d.json", index))
		if err := os.WriteFile(path, receiptWire(t, invocation.AttemptID, invocation.ProviderInvocationID, capability, finalText, `{}`), 0o600); err != nil {
			t.Fatalf("write completion receipt %d: %v", index, err)
		}
		if err := h.loop.ingestProviderCompletionReceiptInvocation(context.Background(), invocation.ProviderInvocationID); err != nil {
			t.Fatalf("ingest completion receipt %d: %v", index, err)
		}
	}
	if _, err := h.loop.convergeProviderTerminalHandoffs(context.Background()); err != nil {
		t.Fatalf("converge invalidated terminal handoff: %v", err)
	}
	settlement, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found {
		t.Fatalf("load invalidated settlement: found=%t err=%v", found, err)
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeFailed || settlement.FailureReasonCode != "provider_incomplete_turn" {
		t.Fatalf("conflicting receipt did not settle fail-closed: %#v", settlement)
	}
}

func TestProviderReceiptIngestionPoisonedFirstInboxDoesNotStarveLaterCandidate(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	first, _, firstInbox := h.addInvocation(t, 1)
	second, secondCapability, secondInbox := h.addInvocation(t, 2)
	if err := os.Remove(firstInbox); err != nil {
		t.Fatalf("remove empty first inbox: %v", err)
	}
	if err := os.Symlink(h.runtimeDir, firstInbox); err != nil {
		t.Fatalf("poison first inbox: %v", err)
	}
	if err := os.WriteFile(filepath.Join(secondInbox, "completion.json"), receiptWire(t, second.AttemptID, second.ProviderInvocationID, secondCapability, "Builder complete.", `{}`), 0o600); err != nil {
		t.Fatalf("write second completion receipt: %v", err)
	}

	if _, err := h.loop.ingestProviderCompletionReceiptPass(context.Background()); err != nil {
		t.Fatalf("bulk ingestion should safely disposition traversal failures: %v", err)
	}
	if _, found, err := h.store.GetProviderCompletionReceiptEvidence(context.Background(), second.ProviderInvocationID); err != nil || !found {
		t.Fatalf("poisoned first inbox starved later receipt: found=%t err=%v", found, err)
	}
	evidence, err := h.store.ListProviderCompletionEvidence(context.Background(), first.ProviderInvocationID)
	if err != nil || len(evidence) != 1 || !strings.Contains(evidence[0].PayloadJSON, `"reason_text":"completion receipt inbox could not be traversed safely"`) {
		t.Fatalf("first inbox did not receive fixed safe traversal disposition: evidence=%#v err=%v", evidence, err)
	}
}

func TestTerminalHandoffDurablyDispositionedInfrastructureErrorDoesNotStarveLaterHandoff(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	first, _, firstInbox := h.addInvocation(t, 1)
	second, _, _ := h.addInvocation(t, 2)
	h.persistHandoffJob(t, first, 1)
	h.persistHandoffJob(t, second, 2)
	if err := os.Remove(firstInbox); err != nil {
		t.Fatalf("remove first inbox: %v", err)
	}
	if err := os.Symlink(h.runtimeDir, firstInbox); err != nil {
		t.Fatalf("poison first inbox: %v", err)
	}

	visited, err := h.loop.convergeProviderTerminalHandoffs(context.Background())
	if err != nil {
		t.Fatalf("durably dispositioned handoff infrastructure error blocked pass: %v", err)
	}
	if visited != 2 || h.loop.terminalHandoffCursor != second.ProviderInvocationID {
		t.Fatalf("handoff pass visited=%d cursor=%q, want 2/%q", visited, h.loop.terminalHandoffCursor, second.ProviderInvocationID)
	}
	if _, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), second.ProviderInvocationID); err != nil || !found {
		t.Fatalf("first poisoned handoff starved later handoff: found=%t err=%v", found, err)
	}
	evidence, err := h.store.ListProviderCompletionEvidence(context.Background(), first.ProviderInvocationID)
	if err != nil || len(evidence) != 1 || !strings.Contains(evidence[0].PayloadJSON, `"reason_code":"receipt_inbox_traversal_failed"`) {
		t.Fatalf("first handoff did not retain its safe infrastructure disposition: evidence=%#v err=%v", evidence, err)
	}
}

func TestTerminalHandoffUnrecoverableInfrastructureErrorRetainsCursor(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	first, _, _ := h.addInvocation(t, 1)
	second, _, _ := h.addInvocation(t, 2)
	h.persistHandoffJob(t, first, 1)
	h.persistHandoffJob(t, second, 2)
	if err := os.WriteFile(providerTerminalHandoffTestPath(h.runtimeDir, first.ProviderInvocationID), []byte(`{`), 0o600); err != nil {
		t.Fatalf("write malformed handoff file: %v", err)
	}

	visited, err := h.loop.convergeProviderTerminalHandoffs(context.Background())
	if err == nil {
		t.Fatal("expected unrecoverable handoff infrastructure error")
	}
	if visited != 0 || h.loop.terminalHandoffCursor != "" {
		t.Fatalf("unrecoverable handoff advanced cursor: visited=%d cursor=%q", visited, h.loop.terminalHandoffCursor)
	}
	if _, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), second.ProviderInvocationID); err != nil || found {
		t.Fatalf("unrecoverable first handoff processed later candidate: found=%t err=%v", found, err)
	}
}

func TestProviderReceiptIngestionIgnoresExactLiveStagingEntry(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	invocation, _, inbox := h.addInvocation(t, 1)
	stagingName := ".receipt-123e4567-e89b-42d3-a456-426614174000.tmp"
	if err := os.WriteFile(filepath.Join(inbox, stagingName), []byte(`{"incomplete":true}`), 0o600); err != nil {
		t.Fatalf("write live staging entry: %v", err)
	}
	if err := h.loop.ingestProviderCompletionReceiptInvocation(context.Background(), invocation.ProviderInvocationID); err != nil {
		t.Fatalf("ingest inbox with live staging entry: %v", err)
	}
	evidence, err := h.store.ListProviderCompletionEvidence(context.Background(), invocation.ProviderInvocationID)
	if err != nil || len(evidence) != 0 {
		t.Fatalf("live staging entry was treated as receipt evidence: evidence=%#v err=%v", evidence, err)
	}
	if _, err := os.Stat(filepath.Join(inbox, stagingName)); err != nil {
		t.Fatalf("live staging entry was removed: %v", err)
	}
}

func TestProviderReceiptIngestionRemovesAbandonedStagingOnlyAfterTerminalSettlement(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	invocation, _, inbox := h.addInvocation(t, 1)
	h.persistHandoffJob(t, invocation, 1)
	stagingPath := filepath.Join(inbox, ".receipt-123e4567-e89b-42d3-a456-426614174000.tmp")
	if err := os.WriteFile(stagingPath, []byte(`{"incomplete":true}`), 0o600); err != nil {
		t.Fatalf("write abandoned staging entry: %v", err)
	}
	if _, err := h.loop.convergeProviderTerminalHandoffs(context.Background()); err != nil {
		t.Fatalf("settle receipt-less terminal handoff: %v", err)
	}
	if _, err := os.Stat(stagingPath); err != nil {
		t.Fatalf("staging entry was removed before terminal cleanup: %v", err)
	}
	if err := h.loop.ingestProviderCompletionReceiptInvocation(context.Background(), invocation.ProviderInvocationID); err != nil {
		t.Fatalf("clean terminal receipt inbox: %v", err)
	}
	if _, err := os.Stat(inbox); !os.IsNotExist(err) {
		t.Fatalf("terminal inbox with abandoned staging entry was not removed: %v", err)
	}
	cleaned, found, err := h.store.GetProviderInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found || cleaned.ReceiptInboxCleanedAt == "" {
		t.Fatalf("terminal staging cleanup was not recorded: found=%t invocation=%#v err=%v", found, cleaned, err)
	}
}

func TestProviderReceiptSpoolSurvivesDeletedDirectAndWorktreeExecutionRoots(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	for index, publishMode := range []string{"direct", "git_worktree"} {
		invocation, capability, inbox := h.addInvocation(t, index+1)
		executionRoot := t.TempDir()
		if strings.HasPrefix(inbox, executionRoot+string(os.PathSeparator)) {
			t.Fatalf("%s receipt inbox resolved inside execution root: %q", publishMode, inbox)
		}
		if !strings.HasPrefix(inbox, filepath.Join(h.runtimeDir, "provider-completion", "receipts")+string(os.PathSeparator)) {
			t.Fatalf("%s receipt inbox did not resolve in central runtime spool: %q", publishMode, inbox)
		}
		if err := os.Remove(executionRoot); err != nil {
			t.Fatalf("delete %s execution root: %v", publishMode, err)
		}
		if err := os.WriteFile(filepath.Join(inbox, "completion.json"), receiptWire(t, invocation.AttemptID, invocation.ProviderInvocationID, capability, "Builder complete.", `{}`), 0o600); err != nil {
			t.Fatalf("write %s central-spool receipt: %v", publishMode, err)
		}
		if err := h.loop.ingestProviderCompletionReceiptInvocation(context.Background(), invocation.ProviderInvocationID); err != nil {
			t.Fatalf("ingest %s receipt after execution-root deletion: %v", publishMode, err)
		}
		if _, found, err := h.store.GetProviderCompletionReceiptEvidence(context.Background(), invocation.ProviderInvocationID); err != nil || !found {
			t.Fatalf("%s receipt spool depended on deleted execution root: found=%t err=%v", publishMode, found, err)
		}
	}
}

func TestStartupReplayCompletesExactReceiptAndHandoffBudget(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	for index := 1; index <= providerlimits.StartupReplayMaxItems; index++ {
		invocation, capability, inbox := h.addInvocation(t, index)
		h.persistHandoffJob(t, invocation, index)
		if err := os.WriteFile(filepath.Join(inbox, "completion.json"), receiptWire(t, invocation.AttemptID, invocation.ProviderInvocationID, capability, "Builder complete.", `{}`), 0o600); err != nil {
			t.Fatalf("write startup receipt %d: %v", index, err)
		}
	}
	runtime := &Runtime{orchestrator: h.loop}
	if err := runtime.recoverDurableExecutionState(context.Background()); err != nil {
		t.Fatalf("recover exact startup receipt and handoff budget: %v", err)
	}
	var settlementCount, handoffCount int
	if err := h.db.QueryRow(`SELECT COUNT(1) FROM provider_completion_settlements`).Scan(&settlementCount); err != nil {
		t.Fatalf("count startup settlements: %v", err)
	}
	if err := h.db.QueryRow(`SELECT COUNT(1) FROM provider_terminal_handoffs`).Scan(&handoffCount); err != nil {
		t.Fatalf("count pending startup handoffs: %v", err)
	}
	if settlementCount != providerlimits.StartupReplayMaxItems || handoffCount != 0 {
		t.Fatalf("startup replay did not converge its exact budget: settlements=%d handoffs=%d", settlementCount, handoffCount)
	}
}

func TestStartupReplayDefersReceiptAndHandoffBeyondBudget(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	for index := 1; index <= providerlimits.StartupReplayMaxItems+1; index++ {
		invocation, capability, inbox := h.addInvocation(t, index)
		h.persistHandoffJob(t, invocation, index)
		if err := os.WriteFile(filepath.Join(inbox, "completion.json"), receiptWire(t, invocation.AttemptID, invocation.ProviderInvocationID, capability, "Builder complete.", `{}`), 0o600); err != nil {
			t.Fatalf("write startup receipt %d: %v", index, err)
		}
	}
	runtime := &Runtime{orchestrator: h.loop}
	if err := runtime.recoverDurableExecutionState(context.Background()); err != nil {
		t.Fatalf("recover bounded startup receipt and handoff work: %v", err)
	}
	var settlementCount, handoffCount int
	if err := h.db.QueryRow(`SELECT COUNT(1) FROM provider_completion_settlements`).Scan(&settlementCount); err != nil {
		t.Fatalf("count bounded startup settlements: %v", err)
	}
	if err := h.db.QueryRow(`SELECT COUNT(1) FROM provider_terminal_handoffs`).Scan(&handoffCount); err != nil {
		t.Fatalf("count deferred startup handoffs: %v", err)
	}
	if settlementCount != 0 || handoffCount != providerlimits.StartupReplayMaxItems+1 {
		t.Fatalf("receipt overflow advanced terminal handoffs: settlements=%d handoffs=%d", settlementCount, handoffCount)
	}
	deferredInvocationID := fmt.Sprintf("pinv_receipt_ingestion_%03d", providerlimits.StartupReplayMaxItems+1)
	deferredInbox, err := h.fsStore.EnsureProviderCompletionInbox(deferredInvocationID)
	if err != nil {
		t.Fatalf("resolve deferred receipt inbox: %v", err)
	}
	deferredReceipt := filepath.Join(deferredInbox, "completion.json")
	if _, err := os.Stat(deferredReceipt); err != nil {
		t.Fatalf("startup replay removed deferred receipt evidence: %v", err)
	}
	for {
		visited, err := h.loop.ingestProviderCompletionReceiptPass(context.Background())
		if err != nil {
			t.Fatalf("normal receipt loop did not continue deferred startup work: %v", err)
		}
		if visited == 0 {
			break
		}
	}
	for {
		visited, err := h.loop.convergeProviderTerminalHandoffs(context.Background())
		if err != nil {
			t.Fatalf("normal handoff loop did not continue deferred startup work: %v", err)
		}
		if visited == 0 {
			break
		}
	}
	if err := h.db.QueryRow(`SELECT COUNT(1) FROM provider_completion_settlements`).Scan(&settlementCount); err != nil {
		t.Fatalf("count converged settlements: %v", err)
	}
	if settlementCount != providerlimits.StartupReplayMaxItems+1 {
		t.Fatalf("normal loops did not converge deferred handoff: settlements=%d", settlementCount)
	}
}

func TestStartupReplayBoundsPendingProviderProcessGuardIngestion(t *testing.T) {
	for _, tc := range []struct {
		name     string
		items    int
		deferred bool
	}{
		{name: "exact budget", items: providerlimits.StartupReplayMaxItems},
		{name: "one item overflow", items: providerlimits.StartupReplayMaxItems + 1, deferred: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newReceiptIngestionHarness(t)
			h.loop.processGuardRuntimeRoot = h.runtimeDir
			for index := 1; index <= tc.items; index++ {
				h.addPendingGuardEvidence(t, index)
			}

			deferred, err := (&Runtime{orchestrator: h.loop}).replayStartupProviderProcessGuards(context.Background())
			if err != nil {
				t.Fatalf("replay pending process guards: %v", err)
			}
			if deferred != tc.deferred {
				t.Fatalf("deferred=%t, want %t", deferred, tc.deferred)
			}
			var quiescent int
			if err := h.db.QueryRow(`SELECT COUNT(1) FROM provider_process_guards WHERE lifecycle = 'quiescent'`).Scan(&quiescent); err != nil {
				t.Fatalf("count ingested process guards: %v", err)
			}
			want := min(tc.items, providerlimits.StartupReplayMaxItems)
			if quiescent != want {
				t.Fatalf("startup ingested %d guards, want %d", quiescent, want)
			}
			if deferred {
				if err := h.loop.ingestPendingProviderProcessGuards(context.Background()); err != nil {
					t.Fatalf("normal process guard loop did not continue deferred work: %v", err)
				}
				if err := h.db.QueryRow(`SELECT COUNT(1) FROM provider_process_guards WHERE lifecycle = 'quiescent'`).Scan(&quiescent); err != nil {
					t.Fatalf("count converged process guards: %v", err)
				}
				if quiescent != tc.items {
					t.Fatalf("normal process guard loop ingested %d guards, want %d", quiescent, tc.items)
				}
			}
		})
	}
}

func TestPendingGuardianEvidenceAbsenceQuarantinesCancelledAttemptWithoutProvingQuiescence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{
			name: "missing evidence",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				if err := os.Remove(path); err != nil {
					t.Fatalf("remove guardian evidence: %v", err)
				}
			},
		},
		{
			name: "corrupt evidence",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				if err := os.WriteFile(path, []byte(`{"type":"result","unexpected":true}`), 0o600); err != nil {
					t.Fatalf("corrupt guardian evidence: %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newReceiptIngestionHarness(t)
			h.loop.processGuardRuntimeRoot = h.runtimeDir
			h.addPendingGuardEvidence(t, 1)
			mustExec(t, h.db, `UPDATE attempt_executions SET launch_started_at = '2026-03-01T12:00:01Z' WHERE attempt_id = 'run_receipt_ingestion:delegated_execution_berserker:1'`)
			const guardID = "guard_startup_replay_001"
			sum := sha256.Sum256([]byte(guardID))
			tc.mutate(t, filepath.Join(h.runtimeDir, "provider-process-guards", "quiescence", fmt.Sprintf("%x.json", sum[:])))
			if err := h.store.QueueAttemptCancel(context.Background(), executionstore.QueueAttemptCancelRequest{
				CancelOutboxID: "cancel_guardian_evidence_lost", AttemptID: "run_receipt_ingestion:delegated_execution_berserker:1", RunID: "run_receipt_ingestion", NodeRunID: "nr_receipt_ingestion_001", ReasonCode: "agent_unresponsive", RequestedAt: "2026-03-01T12:00:02Z", CreatedAt: "2026-03-01T12:00:02Z",
			}); err != nil {
				t.Fatalf("queue cancelled attempt handoff: %v", err)
			}
			h.loop.processGuardAbsent = func(executionstore.ProviderProcessGuard) bool { return true }

			result, err := h.loop.ingestPendingProviderProcessGuardsPass(context.Background(), "", 1, true)
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
			quiescent, err := h.store.AttemptProcessGuardsQuiescent(context.Background(), "run_receipt_ingestion:delegated_execution_berserker:1")
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

func TestStartupReplayBoundsLostTerminalHandoffSettlement(t *testing.T) {
	for _, tc := range []struct {
		name     string
		items    int
		deferred bool
	}{
		{name: "exact budget", items: providerlimits.StartupReplayMaxItems},
		{name: "one item overflow", items: providerlimits.StartupReplayMaxItems + 1, deferred: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newReceiptIngestionHarness(t)
			for index := 1; index <= tc.items; index++ {
				invocation, _, _ := h.addInvocation(t, index)
				h.runningHandoffJob(t, invocation, "", "", index)
			}

			deferred, err := (&Runtime{orchestrator: h.loop}).replayStartupLostProviderTerminalHandoffs(context.Background())
			if err != nil {
				t.Fatalf("replay lost terminal handoffs: %v", err)
			}
			if deferred != tc.deferred {
				t.Fatalf("deferred=%t, want %t", deferred, tc.deferred)
			}
			var settled int
			if err := h.db.QueryRow(`SELECT COUNT(1) FROM attempt_executions WHERE state = 'committed_failure' AND failure_code = 'provider_terminal_handoff_lost'`).Scan(&settled); err != nil {
				t.Fatalf("count settled lost handoffs: %v", err)
			}
			want := min(tc.items, providerlimits.StartupReplayMaxItems)
			if settled != want {
				t.Fatalf("startup settled %d lost handoffs, want %d", settled, want)
			}
			if deferred {
				if err := h.loop.settleLostProviderTerminalHandoffs(context.Background(), false); err != nil {
					t.Fatalf("normal lost-handoff loop did not continue deferred work: %v", err)
				}
				if err := h.db.QueryRow(`SELECT COUNT(1) FROM attempt_executions WHERE state = 'committed_failure' AND failure_code = 'provider_terminal_handoff_lost'`).Scan(&settled); err != nil {
					t.Fatalf("count converged lost handoffs: %v", err)
				}
				if settled != tc.items {
					t.Fatalf("normal lost-handoff loop settled %d items, want %d", settled, tc.items)
				}
			}
		})
	}
}

func TestProviderTerminalHandoffPassVisitsAtMostOneHundredTwentyEight(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	for index := 1; index <= providerTerminalHandoffPassLimit+1; index++ {
		invocation, _, _ := h.addInvocation(t, index)
		h.persistHandoffJob(t, invocation, index)
	}
	visited, err := h.loop.convergeProviderTerminalHandoffs(context.Background())
	if err != nil {
		t.Fatalf("converge bounded terminal handoff pass: %v", err)
	}
	var settlementCount, handoffCount int
	if err := h.db.QueryRow(`SELECT COUNT(1) FROM provider_completion_settlements`).Scan(&settlementCount); err != nil {
		t.Fatalf("count bounded settlements: %v", err)
	}
	if err := h.db.QueryRow(`SELECT COUNT(1) FROM provider_terminal_handoffs`).Scan(&handoffCount); err != nil {
		t.Fatalf("count remaining bounded handoffs: %v", err)
	}
	if visited != providerTerminalHandoffPassLimit || settlementCount != providerTerminalHandoffPassLimit || handoffCount != 1 {
		t.Fatalf("terminal handoff pass exceeded bound: visited=%d settlements=%d handoffs=%d", visited, settlementCount, handoffCount)
	}
}

func TestRecoveryPassFailureRetainsFailedCursorForNormalAndDeferredStartup(t *testing.T) {
	type recoveryPass struct {
		name     string
		seed     func(*testing.T, receiptIngestionHarness)
		arm      func(*testing.T, receiptIngestionHarness)
		drop     func(*testing.T, receiptIngestionHarness)
		normal   func(context.Context, receiptIngestionHarness) (string, error)
		deferred func(*Runtime) startupRecoveryStage
		cursor   func(*Runtime, receiptIngestionHarness) string
		count    func(*testing.T, receiptIngestionHarness) int
	}

	trigger := func(name, table, condition string) (func(*testing.T, receiptIngestionHarness), func(*testing.T, receiptIngestionHarness)) {
		return func(t *testing.T, h receiptIngestionHarness) {
				t.Helper()
				mustExec(t, h.db, `CREATE TRIGGER `+name+` BEFORE INSERT ON `+table+` WHEN `+condition+` BEGIN SELECT RAISE(FAIL, 'forced `+name+` failure'); END`)
			}, func(t *testing.T, h receiptIngestionHarness) {
				t.Helper()
				mustExec(t, h.db, `DROP TRIGGER `+name)
			}
	}
	guardArm := func(t *testing.T, h receiptIngestionHarness) {
		t.Helper()
		mustExec(t, h.db, `CREATE TRIGGER fail_recovery_guard BEFORE UPDATE ON provider_process_guards WHEN NEW.guard_id = 'guard_startup_replay_001' AND NEW.lifecycle = 'quiescent' BEGIN SELECT RAISE(FAIL, 'forced fail_recovery_guard failure'); END`)
	}
	guardDrop := func(t *testing.T, h receiptIngestionHarness) {
		t.Helper()
		mustExec(t, h.db, `DROP TRIGGER fail_recovery_guard`)
	}
	receiptArm, receiptDrop := trigger("fail_recovery_receipt", "provider_completion_evidence", "NEW.provider_invocation_id = 'pinv_receipt_ingestion_001'")
	handoffArm, handoffDrop := trigger("fail_recovery_handoff", "provider_completion_settlements", "NEW.provider_invocation_id = 'pinv_receipt_ingestion_001'")
	lostArm, lostDrop := trigger("fail_recovery_lost_handoff", "provider_completion_settlements", "NEW.provider_invocation_id = 'pinv_receipt_ingestion_001'")

	for _, tc := range []recoveryPass{
		{
			name: "provider guard",
			seed: func(t *testing.T, h receiptIngestionHarness) {
				h.loop.processGuardRuntimeRoot = h.runtimeDir
				h.addPendingGuardEvidence(t, 1)
				h.addPendingGuardEvidence(t, 2)
			},
			arm:  guardArm,
			drop: guardDrop,
			normal: func(ctx context.Context, h receiptIngestionHarness) (string, error) {
				result, err := h.loop.ingestPendingProviderProcessGuardsPass(ctx, "", providerProcessGuardPassLimit, false)
				return result.NextCursor, err
			},
			deferred: func(rt *Runtime) startupRecoveryStage {
				rt.setStartupRecoveryPending(true, true, true, true)
				return startupRecoveryStageGuards
			},
			cursor: func(rt *Runtime, _ receiptIngestionHarness) string { return rt.startupGuardCursor },
			count: func(t *testing.T, h receiptIngestionHarness) int {
				return countReceiptRows(t, h.db, `SELECT COUNT(1) FROM provider_process_guards WHERE lifecycle = 'quiescent'`)
			},
		},
		{
			name: "receipt ingestion",
			seed: func(t *testing.T, h receiptIngestionHarness) {
				for index := 1; index <= 2; index++ {
					invocation, capability, inbox := h.addInvocation(t, index)
					if err := os.WriteFile(filepath.Join(inbox, "completion.json"), receiptWire(t, invocation.AttemptID, invocation.ProviderInvocationID, capability, "Builder complete.", `{}`), 0o600); err != nil {
						t.Fatalf("write completion receipt %d: %v", index, err)
					}
				}
			},
			arm:  receiptArm,
			drop: receiptDrop,
			normal: func(ctx context.Context, h receiptIngestionHarness) (string, error) {
				_, err := h.loop.ingestProviderCompletionReceiptPassWithLimit(ctx, providerReceiptInvocationPassLimit)
				return h.loop.receiptIngestionCursor, err
			},
			deferred: func(rt *Runtime) startupRecoveryStage {
				rt.setStartupRecoveryPending(false, true, true, true)
				return startupRecoveryStageReceipts
			},
			cursor: func(_ *Runtime, h receiptIngestionHarness) string { return h.loop.receiptIngestionCursor },
			count: func(t *testing.T, h receiptIngestionHarness) int {
				return countReceiptRows(t, h.db, `SELECT COUNT(1) FROM provider_completion_evidence WHERE evidence_kind = 'completion_receipt_attempt'`)
			},
		},
		{
			name: "terminal handoff",
			seed: func(t *testing.T, h receiptIngestionHarness) {
				for index := 1; index <= 2; index++ {
					invocation, _, _ := h.addInvocation(t, index)
					h.persistHandoffJob(t, invocation, index)
				}
			},
			arm:  handoffArm,
			drop: handoffDrop,
			normal: func(ctx context.Context, h receiptIngestionHarness) (string, error) {
				_, err := h.loop.convergeProviderTerminalHandoffsWithLimit(ctx, providerTerminalHandoffPassLimit)
				return h.loop.terminalHandoffCursor, err
			},
			deferred: func(rt *Runtime) startupRecoveryStage {
				rt.setStartupRecoveryPending(false, false, true, true)
				return startupRecoveryStageHandoffs
			},
			cursor: func(_ *Runtime, h receiptIngestionHarness) string { return h.loop.terminalHandoffCursor },
			count: func(t *testing.T, h receiptIngestionHarness) int {
				return countReceiptRows(t, h.db, `SELECT COUNT(1) FROM provider_completion_settlements`)
			},
		},
		{
			name: "lost terminal handoff",
			seed: func(t *testing.T, h receiptIngestionHarness) {
				for index := 1; index <= 2; index++ {
					invocation, _, _ := h.addInvocation(t, index)
					h.runningHandoffJob(t, invocation, "", "", index)
				}
			},
			arm:  lostArm,
			drop: lostDrop,
			normal: func(ctx context.Context, h receiptIngestionHarness) (string, error) {
				cursor, _, err := h.loop.settleLostProviderTerminalHandoffsPass(ctx, false, "", providerTerminalHandoffPassLimit)
				return cursor, err
			},
			deferred: func(rt *Runtime) startupRecoveryStage {
				rt.setStartupRecoveryPending(false, false, false, true)
				return startupRecoveryStageLostHandoffs
			},
			cursor: func(rt *Runtime, _ receiptIngestionHarness) string { return rt.startupLostHandoffCursor },
			count: func(t *testing.T, h receiptIngestionHarness) int {
				return countReceiptRows(t, h.db, `SELECT COUNT(1) FROM provider_completion_settlements`)
			},
		},
	} {
		t.Run(tc.name+" normal", func(t *testing.T) {
			h := newReceiptIngestionHarness(t)
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
			h := newReceiptIngestionHarness(t)
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

func TestProviderInvocationActivationFailureRemovesOnlyEmptyInbox(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	mustExec(t, h.db, `CREATE TRIGGER fail_provider_invocation_activation BEFORE INSERT ON provider_invocations BEGIN SELECT RAISE(FAIL, 'forced activation failure'); END`)
	_, _, _, _, err := h.loop.activateProviderInvocationForExecution(context.Background(), runSnapshot{RunID: "run_receipt_ingestion"}, nodeAttempt{NodeRunID: "nr_missing"}, "attempt_activation_failure", "session_activation_failure", executionstore.ProviderCompletionModeReceiptRequired, "2026-03-01T12:00:00Z", "")
	if err == nil {
		t.Fatal("expected provider invocation activation failure")
	}
	receiptRoot := filepath.Join(h.runtimeDir, "provider-completion", "receipts")
	entries, readErr := os.ReadDir(receiptRoot)
	if readErr != nil && !os.IsNotExist(readErr) {
		t.Fatalf("read receipt root after activation failure: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("activation failure left empty inbox debt: entries=%d", len(entries))
	}
}

func (h receiptIngestionHarness) runningHandoffJob(t *testing.T, invocation executionstore.ProviderInvocation, capability, inbox string, index int) asyncExecutionResult {
	t.Helper()
	job := h.baseHandoffJob(invocation, index)
	claim, acquired, err := h.store.ClaimQueuedProviderAttempt(context.Background(), executionstore.ClaimQueuedProviderAttemptRequest{
		AttemptID:  invocation.AttemptID,
		LeaseOwner: job.expectedLeaseOwner,
		LeaseUntil: "2099-03-01T12:10:00Z",
		ClaimedAt:  "2026-03-01T12:00:01Z",
	})
	if err != nil || !acquired {
		t.Fatalf("claim handoff attempt: acquired=%t err=%v", acquired, err)
	}
	job.expectedLeaseVersion = claim.LeaseVersion
	job.expectedLeaseExpiresAt = claim.LeaseExpiresAt
	job.completionCapability = capability
	job.completionReceiptDir = inbox
	job.providerLeaseDuration = 10 * time.Minute
	if _, err := h.db.Exec(`UPDATE attempt_executions SET state = 'running' WHERE attempt_id = ?`, invocation.AttemptID); err != nil {
		t.Fatalf("mark handoff attempt running: %v", err)
	}
	attachQuiescentProviderProcessGuard(t, h.store, invocation.ProviderInvocationID, invocation.AttemptID, invocation.RunID, invocation.NodeRunID, job.expectedLeaseOwner, claim.LeaseVersion, "2026-03-01T12:00:01Z")
	return job
}

func (h receiptIngestionHarness) addPendingGuardEvidence(t *testing.T, index int) {
	t.Helper()
	invocation, _, _ := h.addInvocation(t, index)
	job := h.baseHandoffJob(invocation, index)
	claim, acquired, err := h.store.ClaimQueuedProviderAttempt(context.Background(), executionstore.ClaimQueuedProviderAttemptRequest{
		AttemptID: invocation.AttemptID, LeaseOwner: job.expectedLeaseOwner, LeaseUntil: "2099-03-01T12:10:00Z", ClaimedAt: "2026-03-01T12:00:01Z",
	})
	if err != nil || !acquired {
		t.Fatalf("claim pending process guard attempt: acquired=%t err=%v", acquired, err)
	}
	if _, err := h.db.Exec(`UPDATE attempt_executions SET state = 'running' WHERE attempt_id = ?`, invocation.AttemptID); err != nil {
		t.Fatalf("mark pending process guard attempt running: %v", err)
	}
	guardID := fmt.Sprintf("guard_startup_replay_%03d", index)
	ownership := processguardian.Ownership{
		GuardID: guardID, GuardianPID: index + 1000, GuardianIdentityToken: fmt.Sprintf("guardian-%03d", index),
		ChildPID: index + 2000, ChildIdentityToken: fmt.Sprintf("child-%03d", index), TreeID: index + 2000,
		TreeIdentityToken: fmt.Sprintf("tree-%03d", index), Platform: "unix_process_group", QuiescenceCapability: fmt.Sprintf("capability-%03d", index),
	}
	if _, err := h.store.AcknowledgeProviderProcessGuard(context.Background(), executionstore.AcknowledgeProviderProcessGuardRequest{
		GuardID: ownership.GuardID, ProviderInvocationID: invocation.ProviderInvocationID, AttemptID: invocation.AttemptID, RunID: invocation.RunID, NodeRunID: invocation.NodeRunID,
		CommandOrdinal: 1, Platform: ownership.Platform, GuardianPID: int64(ownership.GuardianPID), GuardianIdentityToken: ownership.GuardianIdentityToken,
		ChildPID: int64(ownership.ChildPID), ChildIdentityToken: ownership.ChildIdentityToken, TreeID: strconv.Itoa(ownership.TreeID), TreeIdentityToken: ownership.TreeIdentityToken,
		QuiescenceCapability: ownership.QuiescenceCapability, ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion,
		StartedAt: "2026-03-01T11:59:59Z", AcknowledgedAt: "2026-03-01T11:59:59Z",
	}); err != nil {
		t.Fatalf("acknowledge pending process guard: %v", err)
	}
	evidence, err := json.Marshal(struct {
		Type   string                 `json:"type"`
		Result processguardian.Result `json:"result"`
	}{Type: "result", Result: processguardian.Result{Ownership: ownership, Quiescent: true}})
	if err != nil {
		t.Fatalf("marshal process guard evidence: %v", err)
	}
	sum := sha256.Sum256([]byte(guardID))
	evidenceDir := filepath.Join(h.runtimeDir, "provider-process-guards", "quiescence")
	if err := os.MkdirAll(evidenceDir, 0o700); err != nil {
		t.Fatalf("create process guard evidence directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(evidenceDir, fmt.Sprintf("%x.json", sum[:])), evidence, 0o600); err != nil {
		t.Fatalf("write process guard evidence: %v", err)
	}
	observed, _, _, err := processguardian.ReadQuiescenceEvidence(h.runtimeDir, guardID)
	if err != nil {
		t.Fatalf("read process guard evidence: %v", err)
	}
	if observed.GuardID != guardID || observed.ChildPID != ownership.ChildPID || observed.TreeID != ownership.TreeID || observed.Platform != ownership.Platform || !observed.Quiescent {
		t.Fatalf("process guard evidence decoded incorrectly: %#v", observed)
	}
}

func (h receiptIngestionHarness) persistHandoffJob(t *testing.T, invocation executionstore.ProviderInvocation, index int) asyncExecutionResult {
	t.Helper()
	job := h.runningHandoffJob(t, invocation, "", "", index)
	zero := 0
	job.providerStreamObservation = opencode.InvocationStreamObservation{
		ProviderSessionID: "",
		ProcessExited:     true,
		ProcessExitCode:   &zero,
		StdoutDrained:     true,
		StderrDrained:     true,
	}
	job.outputText = "provider process output"
	job.outcome = opencode.ExecutionOutcome{State: "success", ReasonCode: "execution_completed"}
	if err := h.loop.persistProviderTerminalHandoff(context.Background(), &job); err != nil {
		t.Fatalf("persist terminal handoff: %v", err)
	}
	return job
}

func (h receiptIngestionHarness) baseHandoffJob(invocation executionstore.ProviderInvocation, index int) asyncExecutionResult {
	return asyncExecutionResult{
		run:                     runSnapshot{RunID: invocation.RunID},
		node:                    nodeDefinition{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker", NodeClass: "delegated_execution"},
		attempt:                 nodeAttempt{NodeRunID: invocation.NodeRunID, NodeID: "delegated_execution_berserker", Attempt: index, RequiredRole: "builder-berserker", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeReceiptRequired},
		attemptID:               invocation.AttemptID,
		providerInvocationID:    invocation.ProviderInvocationID,
		providerInvocationAt:    invocation.CompletionCapabilityIssuedAt,
		completionMode:          executionstore.ProviderCompletionModeReceiptRequired,
		expectedLeaseOwner:      fmt.Sprintf("handoff-worker-%03d", index),
		workspaceID:             invocation.RunID,
		model:                   "test-model",
		occurredAt:              "2026-03-01T12:00:00Z",
		providerDiagnosticsJSON: `{}`,
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

func waitForProviderSettlement(store *executionstore.Store, invocationID string) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), invocationID); err == nil && found {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func providerTerminalHandoffTestPath(runtimeDir, invocationID string) string {
	sum := sha256.Sum256([]byte(invocationID))
	return filepath.Join(runtimeDir, "provider-completion", "handoffs", hex.EncodeToString(sum[:])+".json")
}

func countReceiptRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return count
}
