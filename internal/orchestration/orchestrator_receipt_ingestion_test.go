package orchestration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/schemas"
	executionstore "kingdom/internal/execution/store"
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
	db        *sql.DB
	store     *executionstore.Store
	fsStore   *runtimefs.Store
	loop      *orchestrationLoop
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

	if err := h.loop.ingestProviderCompletionReceipts(context.Background()); err != nil {
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
