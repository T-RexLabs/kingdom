package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestStoreCreateProviderInvocationAndMarkOpen(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:         attemptID,
		RunID:             "run_1",
		NodeRunID:         "nr_1",
		ExecutionMode:     ExecutionModeProviderBacked,
		CompletionMode:    ProviderCompletionModeReceiptRequired,
		SessionRef:        "session_run_1_decree_intake_1",
		ProviderSessionID: "",
		CreatedAt:         "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}

	invocation, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-1",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	})
	if err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if invocation.InvocationState != ProviderInvocationStateIssued {
		t.Fatalf("expected issued invocation state, got %q", invocation.InvocationState)
	}
	if invocation.CompletionCapabilityHash == "" {
		t.Fatal("expected completion capability hash to be stored")
	}
	if invocation.CompletionMode != ProviderCompletionModeReceiptRequired {
		t.Fatalf("expected completion_mode=%q, got %q", ProviderCompletionModeReceiptRequired, invocation.CompletionMode)
	}

	idempotentInvocation, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-1",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	})
	if err != nil {
		t.Fatalf("create provider invocation idempotent retry: %v", err)
	}
	if idempotentInvocation.ProviderInvocationID != invocation.ProviderInvocationID {
		t.Fatalf("expected same invocation on idempotent create, got %#v", idempotentInvocation)
	}

	opened, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_1",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_live_1",
		OpenedAt:             "2026-03-01T12:00:03Z",
	})
	if err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	if opened.InvocationState != ProviderInvocationStateOpen {
		t.Fatalf("expected open invocation state, got %q", opened.InvocationState)
	}
	if opened.ProviderSessionID != "ses_live_1" {
		t.Fatalf("expected provider session id to persist, got %q", opened.ProviderSessionID)
	}
	if opened.OpenedAt != "2026-03-01T12:00:03Z" {
		t.Fatalf("expected opened_at to persist, got %q", opened.OpenedAt)
	}

	attempt, found, err := s.GetAttempt(ctx, attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected attempt to exist")
	}
	if attempt.ActiveProviderInvocationID != "invocation_1" {
		t.Fatalf("expected active provider invocation to be linked, got %q", attempt.ActiveProviderInvocationID)
	}
	if attempt.ProviderSessionID != "ses_live_1" {
		t.Fatalf("expected attempt provider session id to update, got %q", attempt.ProviderSessionID)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM provider_invocations WHERE provider_invocation_id = 'invocation_1'`).Scan(&count); err != nil {
		t.Fatalf("count provider invocations: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one provider invocation row, got %d", count)
	}
}

func TestStoreSettleProviderCompletionAndTerminalCommitsOneCanonicalBoundary(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:atomic_settlement:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: ExecutionModeProviderBacked, CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:01Z"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{ProviderInvocationID: "invocation_atomic_1", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical_atomic", CompletionMode: ProviderCompletionModeServerResponse, IssuedAt: "2026-03-01T12:00:02Z", CreatedAt: "2026-03-01T12:00:02Z"}); err != nil {
		t.Fatalf("create invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{ProviderInvocationID: "invocation_atomic_1", AttemptID: attemptID, ProviderSessionID: "ses_atomic", OpenedAt: "2026-03-01T12:00:03Z"}); err != nil {
		t.Fatalf("open invocation: %v", err)
	}
	settlement, err := s.SettleProviderCompletionAndTerminal(ctx, SettleProviderCompletionAndTerminalRequest{
		Settlement:       FinalizeProviderCompletionSettlementRequest{SettlementID: "settlement_atomic_1", ProviderInvocationID: "invocation_atomic_1", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", Outcome: ProviderCompletionOutcomeSuccess, FinalText: "canonical response", ProviderDiagnosticsJSON: `{}`, SettledAt: "2026-03-01T12:00:04Z"},
		TerminalEvidence: &EnsureTerminalProviderCompletionEvidenceRequest{ProviderInvocationID: "invocation_atomic_1", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", State: "success", OutputText: "canonical response", EvidenceSource: "stream", ProviderDiagnosticsJSON: `{}`, CreatedAt: "2026-03-01T12:00:04Z"},
		Terminal:         CommitTerminalRequest{OutboxID: "outbox_atomic_1", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", TerminalState: "committed_success", PayloadJSON: `{"output_text":"canonical response"}`, CommittedAt: "2026-03-01T12:00:04Z", ProviderSessionID: "ses_atomic"},
	})
	if err != nil {
		t.Fatalf("atomic settlement: %v", err)
	}
	if settlement.PrimaryEvidenceID != TerminalProviderCompletionEvidenceID("invocation_atomic_1") {
		t.Fatalf("unexpected primary evidence: %#v", settlement)
	}
	attempt, found, err := s.GetAttempt(ctx, attemptID)
	if err != nil || !found {
		t.Fatalf("load attempt: found=%t err=%v", found, err)
	}
	if attempt.State != "committed_success" || attempt.CanonicalSettlementID != settlement.SettlementID {
		t.Fatalf("expected committed settled attempt, got %#v", attempt)
	}
	var outboxCount, eventCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_outbox WHERE outbox_id = 'outbox_atomic_1'`).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_event_log WHERE attempt_id = ? AND event_type = 'attempt.succeeded'`, attemptID).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 || eventCount != 1 {
		t.Fatalf("expected terminal handoff and event, outbox=%d events=%d", outboxCount, eventCount)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{ProviderInvocationID: "invocation_atomic_stale", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical_atomic", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:05Z"}); err != nil {
		t.Fatalf("create stale invocation: %v", err)
	}
	_, err = s.SettleProviderCompletionAndTerminal(ctx, SettleProviderCompletionAndTerminalRequest{
		Settlement: FinalizeProviderCompletionSettlementRequest{SettlementID: "settlement_atomic_stale", ProviderInvocationID: "invocation_atomic_stale", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", Outcome: ProviderCompletionOutcomeFailed, PrimaryEvidenceID: "unused", ProviderDiagnosticsJSON: `{}`, SettledAt: "2026-03-01T12:00:06Z"},
		Terminal:   CommitTerminalRequest{OutboxID: "outbox_atomic_stale", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", TerminalState: "committed_failure", PayloadJSON: `{}`, CommittedAt: "2026-03-01T12:00:06Z"},
	})
	var settlementErr *ProviderSettlementError
	if !errors.As(err, &settlementErr) || settlementErr.Reason != "invocation_not_active" {
		t.Fatalf("expected stale invocation to be rejected, got %v", err)
	}
}

func TestStoreProviderInvocationCompletionModeContract(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const serverAttemptID = "run_1:server_response:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      serverAttemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeServerResponse,
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue server-response attempt: %v", err)
	}

	serverInvocation, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_server_response",
		AttemptID:            serverAttemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_server_response",
		CompletionMode:       ProviderCompletionModeServerResponse,
		CreatedAt:            "2026-03-01T12:00:02Z",
	})
	if err != nil {
		t.Fatalf("create server-response invocation: %v", err)
	}
	if serverInvocation.CompletionMode != ProviderCompletionModeServerResponse || serverInvocation.CompletionCapabilityHash != "" || serverInvocation.CompletionCapabilityIssuedAt != "" {
		t.Fatalf("expected capability-free server-response invocation, got %#v", serverInvocation)
	}

	tests := []struct {
		name       string
		mode       ProviderCompletionMode
		capability string
	}{
		{name: "server response with capability", mode: ProviderCompletionModeServerResponse, capability: "forbidden"},
		{name: "receipt required without capability", mode: ProviderCompletionModeReceiptRequired},
		{name: "not applicable", mode: ProviderCompletionModeNotApplicable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
				ProviderInvocationID: "invocation_" + strings.NewReplacer(" ", "_", "-", "_").Replace(tc.name),
				AttemptID:            serverAttemptID,
				RunID:                "run_1",
				NodeRunID:            "nr_1",
				CompletionMode:       tc.mode,
				CompletionCapability: tc.capability,
				CreatedAt:            "2026-03-01T12:00:03Z",
			})
			if err == nil {
				t.Fatal("expected invalid provider invocation completion contract to be rejected")
			}
		})
	}
}

func TestStoreActivateProviderInvocationRejectsAttemptCompletionModeMismatch(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:receipt_required:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue receipt-required attempt: %v", err)
	}

	_, err := s.ActivateProviderInvocation(ctx, ActivateProviderInvocationRequest{
		ProviderInvocationID: "invocation_mismatched_mode",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		CompletionMode:       ProviderCompletionModeServerResponse,
		CreatedAt:            "2026-03-01T12:00:02Z",
		OpenedAt:             "2026-03-01T12:00:02Z",
	})
	var settlementErr *ProviderSettlementError
	if !errors.As(err, &settlementErr) || settlementErr.Reason != "completion_mode_mismatch" {
		t.Fatalf("expected completion mode mismatch, got %v", err)
	}
	if _, found, err := s.GetProviderInvocation(ctx, "invocation_mismatched_mode"); err != nil {
		t.Fatalf("get mismatched invocation: %v", err)
	} else if found {
		t.Fatal("expected mismatched activation to roll back invocation creation")
	}
}

func TestStoreEnsureTerminalProviderCompletionEvidenceReusesExistingEvidence(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_ensure_terminal_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-ensure-terminal-1",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_ensure_terminal_1",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_ensure_terminal_1",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}

	first, err := s.EnsureTerminalProviderCompletionEvidence(ctx, EnsureTerminalProviderCompletionEvidenceRequest{
		ProviderInvocationID: "invocation_ensure_terminal_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		State:                "failed",
		ReasonCode:           "provider_incomplete_turn",
		FailureText:          "provider exited without an accepted kingdom_complete_attempt receipt",
		OutputText:           "Captured provider diagnostic output.",
		EvidenceSource:       "export",
		CreatedAt:            "2026-03-01T12:00:04Z",
	})
	if err != nil {
		t.Fatalf("ensure first terminal evidence: %v", err)
	}

	second, err := s.EnsureTerminalProviderCompletionEvidence(ctx, EnsureTerminalProviderCompletionEvidenceRequest{
		ProviderInvocationID: "invocation_ensure_terminal_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		State:                "failed",
		ReasonCode:           "provider_incomplete_turn",
		FailureText:          "provider exited without an accepted kingdom_complete_attempt receipt",
		OutputText:           "",
		EvidenceSource:       "export",
		CreatedAt:            "2026-03-01T12:00:05Z",
	})
	if err != nil {
		t.Fatalf("ensure second terminal evidence: %v", err)
	}
	if second.EvidenceID != first.EvidenceID {
		t.Fatalf("expected terminal evidence reuse, got first=%q second=%q", first.EvidenceID, second.EvidenceID)
	}
	if second.PayloadJSON != first.PayloadJSON {
		t.Fatalf("expected first terminal evidence payload to be reused, got first=%q second=%q", first.PayloadJSON, second.PayloadJSON)
	}
	evidence, err := s.ListProviderCompletionEvidence(ctx, "invocation_ensure_terminal_1")
	if err != nil {
		t.Fatalf("list provider completion evidence: %v", err)
	}
	if len(evidence) != 1 {
		t.Fatalf("expected exactly one terminal evidence row, got %d", len(evidence))
	}
	if evidence[0].EvidenceKind != ProviderCompletionEvidenceKindExportDiagnostic {
		t.Fatalf("expected terminal evidence kind export_diagnostic, got %#v", evidence[0])
	}
}

func TestStoreEnsureTerminalProviderCompletionEvidenceRejectsUnknownEvidenceSource(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_unknown_terminal_source",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-unknown-terminal-source",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_unknown_terminal_source",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_unknown_terminal_source",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}

	if _, err := s.EnsureTerminalProviderCompletionEvidence(ctx, EnsureTerminalProviderCompletionEvidenceRequest{
		ProviderInvocationID: "invocation_unknown_terminal_source",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		State:                "failed",
		ReasonCode:           "provider_incomplete_turn",
		FailureText:          "provider exited without an accepted kingdom_complete_attempt receipt",
		EvidenceSource:       "unknown",
		CreatedAt:            "2026-03-01T12:00:04Z",
	}); err == nil {
		t.Fatal("expected unknown terminal evidence source to fail")
	}
}

func TestStoreFinalizeProviderCompletionSettlementPersistsProviderDiagnostics(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	const providerInvocationID = "invocation_diagnostics_1"
	const providerDiagnosticsJSON = `{"stderr_tail":"exit status 1"}`
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: providerInvocationID,
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-diagnostics-1",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: providerInvocationID,
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_diagnostics_1",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}

	evidence, err := s.EnsureTerminalProviderCompletionEvidence(ctx, EnsureTerminalProviderCompletionEvidenceRequest{
		ProviderInvocationID:    providerInvocationID,
		AttemptID:               attemptID,
		RunID:                   "run_1",
		NodeRunID:               "nr_1",
		State:                   "failed",
		ReasonCode:              "provider_process_failed",
		FailureText:             "exit status 1",
		OutputText:              "partial output",
		EvidenceSource:          "stream",
		ProviderDiagnosticsJSON: providerDiagnosticsJSON,
		CreatedAt:               "2026-03-01T12:00:04Z",
	})
	if err != nil {
		t.Fatalf("ensure terminal provider completion evidence: %v", err)
	}

	var evidencePayload struct {
		ProviderDiagnostics json.RawMessage `json:"provider_diagnostics"`
	}
	if err := json.Unmarshal([]byte(evidence.PayloadJSON), &evidencePayload); err != nil {
		t.Fatalf("decode terminal evidence payload: %v", err)
	}
	if string(evidencePayload.ProviderDiagnostics) != providerDiagnosticsJSON {
		t.Fatalf("expected provider diagnostics payload %s, got %s", providerDiagnosticsJSON, string(evidencePayload.ProviderDiagnostics))
	}

	settlement, err := s.FinalizeProviderCompletionSettlement(ctx, FinalizeProviderCompletionSettlementRequest{
		SettlementID:            ProviderCompletionSettlementID(providerInvocationID),
		ProviderInvocationID:    providerInvocationID,
		AttemptID:               attemptID,
		RunID:                   "run_1",
		NodeRunID:               "nr_1",
		Outcome:                 ProviderCompletionOutcomeFailed,
		PrimaryEvidenceID:       evidence.EvidenceID,
		StructuredPayloadJSON:   "{}",
		ProviderDiagnosticsJSON: providerDiagnosticsJSON,
		FailureReasonCode:       "provider_process_failed",
		FailureReasonText:       "exit status 1",
		SettledAt:               "2026-03-01T12:00:05Z",
	})
	if err != nil {
		t.Fatalf("finalize provider completion settlement: %v", err)
	}
	if settlement.ProviderDiagnosticsJSON != providerDiagnosticsJSON {
		t.Fatalf("expected settlement diagnostics %s, got %s", providerDiagnosticsJSON, settlement.ProviderDiagnosticsJSON)
	}

	stored, found, err := s.GetProviderCompletionSettlementByInvocation(ctx, providerInvocationID)
	if err != nil {
		t.Fatalf("get stored provider completion settlement: %v", err)
	}
	if !found {
		t.Fatal("expected provider completion settlement to exist")
	}
	if stored.ProviderDiagnosticsJSON != providerDiagnosticsJSON {
		t.Fatalf("expected stored settlement diagnostics %s, got %s", providerDiagnosticsJSON, stored.ProviderDiagnosticsJSON)
	}
}

func TestStoreMarkProviderInvocationOpenRejectsProviderSessionLineageConflict(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_lineage_conflict",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-lineage-conflict",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_lineage_conflict",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_live_1",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}

	_, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_lineage_conflict",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_live_2",
		OpenedAt:             "2026-03-01T12:00:04Z",
	})
	if err == nil {
		t.Fatal("expected provider session lineage conflict to fail")
	}
	var settlementErr *ProviderSettlementError
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected provider settlement error, got %T: %v", err, err)
	}
	if settlementErr.Reason != "provider_session_lineage_conflict" {
		t.Fatalf("expected provider_session_lineage_conflict reason, got %q", settlementErr.Reason)
	}

	invocation, found, err := s.GetProviderInvocation(ctx, "invocation_lineage_conflict")
	if err != nil {
		t.Fatalf("get provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider invocation to exist")
	}
	if invocation.ProviderSessionID != "ses_live_1" {
		t.Fatalf("expected original provider session id to remain authoritative, got %#v", invocation)
	}

	attempt, found, err := s.GetAttempt(ctx, attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected attempt to exist")
	}
	if attempt.ProviderSessionID != "ses_live_1" {
		t.Fatalf("expected attempt provider session id to remain unchanged, got %#v", attempt)
	}
}

func TestStoreAcceptProviderCompletionReceiptIdempotentAndConflicting(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_receipt_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-receipt-1",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_receipt_1",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_receipt_1",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}

	accepted, err := s.AcceptProviderCompletionReceipt(ctx, AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_evidence_1",
		ProviderInvocationID:  "invocation_receipt_1",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_1",
		ProtocolVersion:       "v1",
		CompletionCapability:  "capability-receipt-1",
		FinalText:             "  Completed successfully.  ",
		StructuredPayloadJSON: `{"verdict":"PASS"}`,
		ReceivedAt:            "2026-03-01T12:00:04Z",
	})
	if err != nil {
		t.Fatalf("accept provider completion receipt: %v", err)
	}
	if accepted.Idempotent {
		t.Fatal("expected first completion receipt accept to be non-idempotent")
	}
	if accepted.Invocation.InvocationState != ProviderInvocationStateReceiptPersisted {
		t.Fatalf("expected receipt_persisted invocation state, got %q", accepted.Invocation.InvocationState)
	}
	if accepted.Evidence.EvidenceID != "receipt_evidence_1" {
		t.Fatalf("expected first evidence id to persist, got %q", accepted.Evidence.EvidenceID)
	}

	retry, err := s.AcceptProviderCompletionReceipt(ctx, AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_evidence_retry",
		ProviderInvocationID:  "invocation_receipt_1",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_1",
		ProtocolVersion:       "v1",
		CompletionCapability:  "capability-receipt-1",
		FinalText:             "Completed successfully.",
		StructuredPayloadJSON: `{"verdict":"PASS"}`,
		ReceivedAt:            "2026-03-01T12:00:05Z",
	})
	if err != nil {
		t.Fatalf("accept provider completion receipt retry: %v", err)
	}
	if !retry.Idempotent {
		t.Fatal("expected exact duplicate completion receipt to be idempotent")
	}
	if retry.Evidence.EvidenceID != "receipt_evidence_1" {
		t.Fatalf("expected idempotent retry to return the original evidence, got %q", retry.Evidence.EvidenceID)
	}

	_, err = s.AcceptProviderCompletionReceipt(ctx, AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_evidence_conflict",
		ProviderInvocationID:  "invocation_receipt_1",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_1",
		ProtocolVersion:       "v1",
		CompletionCapability:  "capability-receipt-1",
		FinalText:             "Conflicting settled answer.",
		StructuredPayloadJSON: `{"verdict":"PASS"}`,
		ReceivedAt:            "2026-03-01T12:00:06Z",
	})
	if err == nil {
		t.Fatal("expected conflicting completion receipt to fail")
	}
	var settlementErr *ProviderSettlementError
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected provider settlement error, got %T: %v", err, err)
	}
	if settlementErr.Reason != "completion_receipt_conflict" {
		t.Fatalf("expected completion_receipt_conflict reason, got %q", settlementErr.Reason)
	}

	invocation, found, err := s.GetProviderInvocation(ctx, "invocation_receipt_1")
	if err != nil {
		t.Fatalf("get provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider invocation to exist")
	}
	if invocation.InvocationState != ProviderInvocationStateInvalidHandshake {
		t.Fatalf("expected conflicting completion receipt to invalidate the handshake, got %q", invocation.InvocationState)
	}
	if invocation.InvalidReason != "completion_receipt_conflict" {
		t.Fatalf("expected invalid reason to persist, got %q", invocation.InvalidReason)
	}

	_, err = s.AcceptProviderCompletionReceipt(ctx, AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_evidence_after_invalid_handshake",
		ProviderInvocationID:  "invocation_receipt_1",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_1",
		ProtocolVersion:       "v1",
		CompletionCapability:  "capability-receipt-1",
		FinalText:             "Completed successfully.",
		StructuredPayloadJSON: `{"verdict":"PASS"}`,
		ReceivedAt:            "2026-03-01T12:00:07Z",
	})
	if err == nil {
		t.Fatal("expected duplicate receipt after invalid handshake to fail")
	}
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected provider settlement error after invalid handshake, got %T: %v", err, err)
	}
	if settlementErr.Reason != "invocation_invalid_handshake" {
		t.Fatalf("expected invocation_invalid_handshake reason, got %q", settlementErr.Reason)
	}

	evidenceRows, err := s.ListProviderCompletionEvidence(ctx, "invocation_receipt_1")
	if err != nil {
		t.Fatalf("list provider completion evidence: %v", err)
	}
	if len(evidenceRows) != 1 {
		t.Fatalf("expected exactly one accepted receipt evidence row, got %d", len(evidenceRows))
	}
}

func TestStoreAcceptProviderCompletionReceiptRejectsDuplicateWhenInvocationNoLongerActive(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_receipt_old",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-receipt-old",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("create old provider invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_receipt_old",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_receipt_old",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark old provider invocation open: %v", err)
	}
	if _, err := s.AcceptProviderCompletionReceipt(ctx, AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_evidence_old",
		ProviderInvocationID:  "invocation_receipt_old",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_1",
		ProtocolVersion:       "v1",
		CompletionCapability:  "capability-receipt-old",
		FinalText:             "Completed successfully.",
		StructuredPayloadJSON: `{"verdict":"PASS"}`,
		ReceivedAt:            "2026-03-01T12:00:04Z",
	}); err != nil {
		t.Fatalf("accept initial provider receipt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_receipt_new",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-receipt-new",
		IssuedAt:             "2026-03-01T12:00:05Z",
		CreatedAt:            "2026-03-01T12:00:05Z",
	}); err != nil {
		t.Fatalf("create new provider invocation: %v", err)
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET active_provider_invocation_id = ? WHERE attempt_id = ?`, "invocation_receipt_new", attemptID); err != nil {
		t.Fatalf("switch active provider invocation: %v", err)
	}

	_, err := s.AcceptProviderCompletionReceipt(ctx, AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_evidence_old_duplicate",
		ProviderInvocationID:  "invocation_receipt_old",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_1",
		ProtocolVersion:       "v1",
		CompletionCapability:  "capability-receipt-old",
		FinalText:             "Completed successfully.",
		StructuredPayloadJSON: `{"verdict":"PASS"}`,
		ReceivedAt:            "2026-03-01T12:00:06Z",
	})
	if err == nil {
		t.Fatal("expected duplicate receipt on inactive invocation to fail")
	}
	var settlementErr *ProviderSettlementError
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected provider settlement error, got %T: %v", err, err)
	}
	if settlementErr.Reason != "invocation_not_active" {
		t.Fatalf("expected invocation_not_active reason, got %q", settlementErr.Reason)
	}
}

func TestStoreObserveProviderCompletionReceiptAttemptPersistsEvidenceAndCounters(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_receipt_attempt_obs",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-receipt-attempt-obs",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_receipt_attempt_obs",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_receipt_attempt_obs",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}

	if _, err := s.ObserveProviderCompletionReceiptAttempt(ctx, ObserveProviderCompletionReceiptAttemptRequest{
		EvidenceID:            "receipt_attempt_obs_1",
		ProviderInvocationID:  "invocation_receipt_attempt_obs",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_1",
		ProtocolVersion:       "v1",
		ObservedAttemptID:     "run_1:decree_intake:999",
		ObservedInvocationID:  "invocation_receipt_attempt_obs",
		CompletionCapability:  "capability-receipt-attempt-obs",
		FinalText:             "bad receipt",
		StructuredPayloadJSON: `{"verdict":"PASS"}`,
		Disposition:           ProviderCompletionReceiptDispositionRejected,
		ReasonCode:            "validation_failed",
		ReasonText:            "completion receipt final_text is required",
		ObservedAt:            "2026-03-01T12:00:04Z",
	}); err != nil {
		t.Fatalf("observe rejected receipt attempt: %v", err)
	}
	if _, err := s.ObserveProviderCompletionReceiptAttempt(ctx, ObserveProviderCompletionReceiptAttemptRequest{
		EvidenceID:            "receipt_attempt_obs_2",
		ProviderInvocationID:  "invocation_receipt_attempt_obs",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_1",
		ProtocolVersion:       "v1",
		ObservedAttemptID:     attemptID,
		ObservedInvocationID:  "invocation_receipt_attempt_obs",
		CompletionCapability:  "capability-receipt-attempt-obs",
		FinalText:             "good receipt",
		StructuredPayloadJSON: `{"verdict":"PASS"}`,
		Disposition:           ProviderCompletionReceiptDispositionAccepted,
		ReasonCode:            "accepted",
		AcceptedEvidenceID:    "receipt_evidence_obs_accepted",
		ObservedAt:            "2026-03-01T12:00:05Z",
	}); err != nil {
		t.Fatalf("observe accepted receipt attempt: %v", err)
	}

	evidenceRows, err := s.ListProviderCompletionEvidence(ctx, "invocation_receipt_attempt_obs")
	if err != nil {
		t.Fatalf("list provider completion evidence: %v", err)
	}
	if len(evidenceRows) != 2 {
		t.Fatalf("expected two receipt attempt evidence rows, got %d", len(evidenceRows))
	}
	if evidenceRows[0].EvidenceKind != ProviderCompletionEvidenceKindCompletionReceiptAttempt || evidenceRows[1].EvidenceKind != ProviderCompletionEvidenceKindCompletionReceiptAttempt {
		t.Fatalf("expected receipt attempt evidence kinds, got %#v", evidenceRows)
	}
	var payload struct {
		Disposition        string `json:"disposition"`
		ReasonCode         string `json:"reason_code"`
		AcceptedEvidenceID string `json:"accepted_evidence_id"`
	}
	if err := json.Unmarshal([]byte(evidenceRows[1].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode receipt attempt payload: %v", err)
	}
	if payload.Disposition != string(ProviderCompletionReceiptDispositionAccepted) {
		t.Fatalf("expected accepted disposition, got %q", payload.Disposition)
	}
	if payload.AcceptedEvidenceID != "receipt_evidence_obs_accepted" {
		t.Fatalf("expected accepted evidence id in payload, got %q", payload.AcceptedEvidenceID)
	}

	invocation, found, err := s.GetProviderInvocation(ctx, "invocation_receipt_attempt_obs")
	if err != nil {
		t.Fatalf("get provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider invocation to exist")
	}
	if invocation.ReceiptSeenCount != 2 {
		t.Fatalf("expected receipt_seen_count=2, got %d", invocation.ReceiptSeenCount)
	}
	if invocation.FirstReceiptSeenAt != "2026-03-01T12:00:04Z" {
		t.Fatalf("expected first_receipt_seen_at to persist, got %q", invocation.FirstReceiptSeenAt)
	}
	if invocation.LastReceiptSeenAt != "2026-03-01T12:00:05Z" {
		t.Fatalf("expected last_receipt_seen_at to persist, got %q", invocation.LastReceiptSeenAt)
	}
	if invocation.LastReceiptDisposition != ProviderCompletionReceiptDispositionAccepted {
		t.Fatalf("expected last receipt disposition accepted, got %q", invocation.LastReceiptDisposition)
	}
}

func TestStoreAcceptProviderCompletionReceiptInvalidatesAttemptMismatch(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_attempt_mismatch",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-attempt-mismatch",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_attempt_mismatch",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_attempt_mismatch",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}

	_, err := s.AcceptProviderCompletionReceipt(ctx, AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_evidence_attempt_mismatch",
		ProviderInvocationID:  "invocation_attempt_mismatch",
		AttemptID:             "run_1:decree_intake:999",
		RunID:                 "run_1",
		NodeRunID:             "nr_1",
		ProtocolVersion:       "v1",
		CompletionCapability:  "capability-attempt-mismatch",
		FinalText:             "Completed successfully.",
		StructuredPayloadJSON: `{"verdict":"PASS"}`,
		ReceivedAt:            "2026-03-01T12:00:04Z",
	})
	if err == nil {
		t.Fatal("expected mismatched attempt receipt to fail")
	}
	var settlementErr *ProviderSettlementError
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected provider settlement error, got %T: %v", err, err)
	}
	if settlementErr.Reason != "attempt_lineage_mismatch" {
		t.Fatalf("expected attempt_lineage_mismatch reason, got %q", settlementErr.Reason)
	}

	invocation, found, err := s.GetProviderInvocation(ctx, "invocation_attempt_mismatch")
	if err != nil {
		t.Fatalf("get provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider invocation to exist")
	}
	if invocation.InvocationState != ProviderInvocationStateInvalidHandshake {
		t.Fatalf("expected attempt mismatch to invalidate the handshake, got %q", invocation.InvocationState)
	}
	if invocation.InvalidReason != "attempt_lineage_mismatch" {
		t.Fatalf("expected attempt mismatch invalid reason to persist, got %q", invocation.InvalidReason)
	}

	evidenceRows, err := s.ListProviderCompletionEvidence(ctx, "invocation_attempt_mismatch")
	if err != nil {
		t.Fatalf("list provider completion evidence: %v", err)
	}
	if len(evidenceRows) != 0 {
		t.Fatalf("expected no receipt evidence rows after attempt mismatch, got %d", len(evidenceRows))
	}
}

func TestStoreAcceptProviderCompletionReceiptInvalidatesCompletionCapabilityMismatch(t *testing.T) {
	t.Parallel()

	setupOpenInvocation := func(t *testing.T, attemptID, invocationID, capability string) (*Store, context.Context) {
		t.Helper()

		db := openTestDB(t)
		applyMigration(t, db, "0001_initial_schema.up.sql")
		seedMinimumGraph(t, db)

		s := New(db)
		ctx := context.Background()
		if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
			AttemptID:      attemptID,
			RunID:          "run_1",
			NodeRunID:      "nr_1",
			ExecutionMode:  ExecutionModeProviderBacked,
			CompletionMode: ProviderCompletionModeReceiptRequired,
			SessionRef:     "session_run_1_decree_intake_1",
			CreatedAt:      "2026-03-01T12:00:01Z",
		}); err != nil {
			t.Fatalf("enqueue attempt: %v", err)
		}
		if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
			ProviderInvocationID: invocationID,
			AttemptID:            attemptID,
			RunID:                "run_1",
			NodeRunID:            "nr_1",
			LogicalSessionID:     "session_run_1_decree_intake_1",
			CompletionMode:       ProviderCompletionModeReceiptRequired,
			CompletionCapability: capability,
			IssuedAt:             "2026-03-01T12:00:02Z",
			CreatedAt:            "2026-03-01T12:00:02Z",
		}); err != nil {
			t.Fatalf("create provider invocation: %v", err)
		}
		if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
			ProviderInvocationID: invocationID,
			AttemptID:            attemptID,
			ProviderSessionID:    "ses_" + invocationID,
			OpenedAt:             "2026-03-01T12:00:03Z",
		}); err != nil {
			t.Fatalf("mark provider invocation open: %v", err)
		}

		return s, ctx
	}

	t.Run("mismatch invalidates invocation", func(t *testing.T) {
		s, ctx := setupOpenInvocation(t, "run_1:decree_intake:1", "invocation_capability_mismatch", "capability-expected")

		_, err := s.AcceptProviderCompletionReceipt(ctx, AcceptProviderCompletionReceiptRequest{
			EvidenceID:            "receipt_evidence_capability_mismatch",
			ProviderInvocationID:  "invocation_capability_mismatch",
			AttemptID:             "run_1:decree_intake:1",
			RunID:                 "run_1",
			NodeRunID:             "nr_1",
			ProtocolVersion:       "v1",
			CompletionCapability:  "capability-wrong",
			FinalText:             "Completed successfully.",
			StructuredPayloadJSON: `{"verdict":"PASS"}`,
			ReceivedAt:            "2026-03-01T12:00:04Z",
		})
		if err == nil {
			t.Fatal("expected mismatched capability receipt to fail")
		}
		var settlementErr *ProviderSettlementError
		if !errors.As(err, &settlementErr) {
			t.Fatalf("expected provider settlement error, got %T: %v", err, err)
		}
		if settlementErr.Reason != "completion_capability_mismatch" {
			t.Fatalf("expected completion_capability_mismatch reason, got %q", settlementErr.Reason)
		}

		invocation, found, err := s.GetProviderInvocation(ctx, "invocation_capability_mismatch")
		if err != nil {
			t.Fatalf("get provider invocation: %v", err)
		}
		if !found {
			t.Fatal("expected provider invocation to exist")
		}
		if invocation.InvocationState != ProviderInvocationStateInvalidHandshake {
			t.Fatalf("expected capability mismatch to invalidate the handshake, got %q", invocation.InvocationState)
		}
		if invocation.InvalidReason != "completion_capability_mismatch" {
			t.Fatalf("expected capability mismatch invalid reason to persist, got %q", invocation.InvalidReason)
		}

		evidenceRows, err := s.ListProviderCompletionEvidence(ctx, "invocation_capability_mismatch")
		if err != nil {
			t.Fatalf("list provider completion evidence: %v", err)
		}
		if len(evidenceRows) != 0 {
			t.Fatalf("expected no receipt evidence rows after capability mismatch, got %d", len(evidenceRows))
		}
	})

	t.Run("matching capability still accepts", func(t *testing.T) {
		s, ctx := setupOpenInvocation(t, "run_1:decree_intake:1", "invocation_capability_match", "capability-match")

		accepted, err := s.AcceptProviderCompletionReceipt(ctx, AcceptProviderCompletionReceiptRequest{
			EvidenceID:            "receipt_evidence_capability_match",
			ProviderInvocationID:  "invocation_capability_match",
			AttemptID:             "run_1:decree_intake:1",
			RunID:                 "run_1",
			NodeRunID:             "nr_1",
			ProtocolVersion:       "v1",
			CompletionCapability:  "capability-match",
			FinalText:             "Completed successfully.",
			StructuredPayloadJSON: `{"verdict":"PASS"}`,
			ReceivedAt:            "2026-03-01T12:00:04Z",
		})
		if err != nil {
			t.Fatalf("accept provider completion receipt: %v", err)
		}
		if accepted.Idempotent {
			t.Fatal("expected first matching completion receipt accept to be non-idempotent")
		}
		if accepted.Invocation.InvocationState != ProviderInvocationStateReceiptPersisted {
			t.Fatalf("expected receipt_persisted invocation state, got %q", accepted.Invocation.InvocationState)
		}
		if accepted.Evidence.EvidenceID != "receipt_evidence_capability_match" {
			t.Fatalf("expected accepted receipt evidence id to persist, got %q", accepted.Evidence.EvidenceID)
		}
	})
}

func TestStoreActivateProviderInvocationAtomicallyReplacesActiveInvocation(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_old",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-old",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("create old provider invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_old",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_old",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark old provider invocation open: %v", err)
	}

	activated, err := s.ActivateProviderInvocation(ctx, ActivateProviderInvocationRequest{
		ProviderInvocationID:         "invocation_new",
		AttemptID:                    attemptID,
		RunID:                        "run_1",
		NodeRunID:                    "nr_1",
		LogicalSessionID:             "session_run_1_decree_intake_1",
		CompletionMode:               ProviderCompletionModeReceiptRequired,
		CompletionCapability:         "capability-new",
		IssuedAt:                     "2026-03-01T12:00:04Z",
		CreatedAt:                    "2026-03-01T12:00:04Z",
		OpenedAt:                     "2026-03-01T12:00:04Z",
		ReplacesProviderInvocationID: "invocation_old",
	})
	if err != nil {
		t.Fatalf("activate replacement provider invocation: %v", err)
	}
	if activated.ProviderInvocationID != "invocation_new" || activated.InvocationState != ProviderInvocationStateOpen {
		t.Fatalf("expected new open provider invocation, got %#v", activated)
	}

	attempt, found, err := s.GetAttempt(ctx, attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected attempt to exist")
	}
	if attempt.ActiveProviderInvocationID != "invocation_new" {
		t.Fatalf("expected active invocation to switch to replacement, got %#v", attempt)
	}

	oldInvocation, found, err := s.GetProviderInvocation(ctx, "invocation_old")
	if err != nil {
		t.Fatalf("get old provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected old provider invocation to exist")
	}
	if oldInvocation.InvocationState != ProviderInvocationStateSuperseded || oldInvocation.SupersededByInvocationID != "invocation_new" {
		t.Fatalf("expected old invocation to be superseded by replacement, got %#v", oldInvocation)
	}
}

func TestStoreActivateProviderInvocationRollsBackOnReplacementConflict(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_old",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-old",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("create old provider invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_old",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_old",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark old provider invocation open: %v", err)
	}

	_, err := s.ActivateProviderInvocation(ctx, ActivateProviderInvocationRequest{
		ProviderInvocationID:         "invocation_new",
		AttemptID:                    attemptID,
		RunID:                        "run_1",
		NodeRunID:                    "nr_1",
		LogicalSessionID:             "session_run_1_decree_intake_1",
		CompletionMode:               ProviderCompletionModeReceiptRequired,
		CompletionCapability:         "capability-new",
		IssuedAt:                     "2026-03-01T12:00:04Z",
		CreatedAt:                    "2026-03-01T12:00:04Z",
		OpenedAt:                     "2026-03-01T12:00:04Z",
		ReplacesProviderInvocationID: "invocation_missing",
	})
	if err == nil {
		t.Fatal("expected replacement conflict to fail activation")
	}

	if _, found, err := s.GetProviderInvocation(ctx, "invocation_new"); err != nil {
		t.Fatalf("get rolled back provider invocation: %v", err)
	} else if found {
		t.Fatal("expected replacement activation rollback to remove new invocation")
	}
	attempt, found, err := s.GetAttempt(ctx, attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected attempt to exist")
	}
	if attempt.ActiveProviderInvocationID != "invocation_old" {
		t.Fatalf("expected old invocation to remain active after rollback, got %#v", attempt)
	}
}

func TestStoreSupersedeProviderInvocationRejectsStaleReceipt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	for _, invocationID := range []string{"invocation_old", "invocation_new"} {
		if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
			ProviderInvocationID: invocationID,
			AttemptID:            attemptID,
			RunID:                "run_1",
			NodeRunID:            "nr_1",
			LogicalSessionID:     "session_run_1_decree_intake_1",
			CompletionMode:       ProviderCompletionModeReceiptRequired,
			CompletionCapability: invocationID + "-capability",
			IssuedAt:             "2026-03-01T12:00:02Z",
			CreatedAt:            "2026-03-01T12:00:02Z",
		}); err != nil {
			t.Fatalf("create provider invocation %q: %v", invocationID, err)
		}
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_old",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_old",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark old provider invocation open: %v", err)
	}
	if err := s.SupersedeProviderInvocation(ctx, SupersedeProviderInvocationRequest{
		ProviderInvocationID:     "invocation_old",
		SupersededByInvocationID: "invocation_new",
		UpdatedAt:                "2026-03-01T12:00:04Z",
	}); err != nil {
		t.Fatalf("supersede provider invocation: %v", err)
	}

	attempt, found, err := s.GetAttempt(ctx, attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected attempt to exist")
	}
	if attempt.ActiveProviderInvocationID != "invocation_new" {
		t.Fatalf("expected attempt active invocation to switch to replacement, got %q", attempt.ActiveProviderInvocationID)
	}

	oldInvocation, found, err := s.GetProviderInvocation(ctx, "invocation_old")
	if err != nil {
		t.Fatalf("get superseded provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected superseded provider invocation to exist")
	}
	if oldInvocation.InvocationState != ProviderInvocationStateSuperseded {
		t.Fatalf("expected superseded invocation state, got %q", oldInvocation.InvocationState)
	}
	if oldInvocation.SupersededByInvocationID != "invocation_new" {
		t.Fatalf("expected superseded_by_invocation_id to persist, got %q", oldInvocation.SupersededByInvocationID)
	}

	_, err = s.AcceptProviderCompletionReceipt(ctx, AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "stale_receipt_evidence",
		ProviderInvocationID:  "invocation_old",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_1",
		ProtocolVersion:       "v1",
		CompletionCapability:  "invocation_old-capability",
		FinalText:             "stale receipt",
		StructuredPayloadJSON: `{}`,
		ReceivedAt:            "2026-03-01T12:00:05Z",
	})
	if err == nil {
		t.Fatal("expected stale superseded receipt to fail")
	}
	var settlementErr *ProviderSettlementError
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected provider settlement error, got %T: %v", err, err)
	}
	if settlementErr.Reason != "invocation_superseded" {
		t.Fatalf("expected invocation_superseded reason, got %q", settlementErr.Reason)
	}
}

func TestStoreAcceptProviderCompletionReceiptRejectsTerminalSupersededAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:2"
	if _, err := db.Exec(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version) VALUES ('nr_retry_provider_2', 'run_1', 'decree_intake', 2, 2, 'blocked', 'king', 'agent_king', 1)`); err != nil {
		t.Fatalf("insert superseded retry node run: %v", err)
	}
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_retry_provider_2",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		SessionRef:     "session_run_1_decree_intake_2",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_retry_2",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_retry_provider_2",
		LogicalSessionID:     "session_run_1_decree_intake_2",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-retry-2",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_retry_2",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_retry_2",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	if _, err := db.Exec(`UPDATE attempt_executions
		SET state = 'committed_cancelled',
		    terminal_observed_at = '2026-03-01T12:00:04Z',
		    committed_at = '2026-03-01T12:00:04Z',
		    failure_code = 'superseded_after_launch_handoff',
		    failure_text = 'retry attempt superseded after launch handoff by an earlier successful attempt',
		    updated_at = '2026-03-01T12:00:04Z'
		WHERE attempt_id = ?`, attemptID); err != nil {
		t.Fatalf("terminalize superseded attempt: %v", err)
	}

	_, err := s.AcceptProviderCompletionReceipt(ctx, AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "stale_terminal_receipt_evidence",
		ProviderInvocationID:  "invocation_retry_2",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_retry_provider_2",
		ProtocolVersion:       "v1",
		CompletionCapability:  "capability-retry-2",
		FinalText:             "stale receipt",
		StructuredPayloadJSON: `{}`,
		ReceivedAt:            "2026-03-01T12:00:05Z",
	})
	if err == nil {
		t.Fatal("expected stale receipt for terminal superseded attempt to fail")
	}
	var settlementErr *ProviderSettlementError
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected provider settlement error, got %T: %v", err, err)
	}
	if settlementErr.Reason != "attempt_not_active" {
		t.Fatalf("expected attempt_not_active reason, got %q", settlementErr.Reason)
	}
}

func TestStoreFinalizeProviderCompletionSettlementSuccessLinksCanonicalAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_success_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-success-1",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_success_1",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_success_1",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	receipt, err := s.AcceptProviderCompletionReceipt(ctx, AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_success_1",
		ProviderInvocationID:  "invocation_success_1",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_1",
		ProtocolVersion:       "v1",
		CompletionCapability:  "capability-success-1",
		FinalText:             "success response",
		StructuredPayloadJSON: `{"verdict":"PASS"}`,
		ReceivedAt:            "2026-03-01T12:00:04Z",
	})
	if err != nil {
		t.Fatalf("accept provider completion receipt: %v", err)
	}

	settlement, err := s.FinalizeProviderCompletionSettlement(ctx, FinalizeProviderCompletionSettlementRequest{
		SettlementID:         "settlement_success_1",
		ProviderInvocationID: "invocation_success_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		Outcome:              ProviderCompletionOutcomeSuccess,
		PrimaryEvidenceID:    receipt.Evidence.EvidenceID,
		SettledAt:            "2026-03-01T12:00:05Z",
	})
	if err != nil {
		t.Fatalf("finalize provider completion settlement: %v", err)
	}
	if settlement.Outcome != ProviderCompletionOutcomeSuccess {
		t.Fatalf("expected success settlement outcome, got %q", settlement.Outcome)
	}
	if settlement.FinalText != "success response" {
		t.Fatalf("expected settlement final text from receipt evidence, got %q", settlement.FinalText)
	}
	if settlement.StructuredPayloadJSON != `{"verdict":"PASS"}` {
		t.Fatalf("expected structured payload json to round trip canonically, got %q", settlement.StructuredPayloadJSON)
	}

	idempotentSettlement, err := s.FinalizeProviderCompletionSettlement(ctx, FinalizeProviderCompletionSettlementRequest{
		SettlementID:         "settlement_success_1",
		ProviderInvocationID: "invocation_success_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		Outcome:              ProviderCompletionOutcomeSuccess,
		PrimaryEvidenceID:    receipt.Evidence.EvidenceID,
		SettledAt:            "2026-03-01T12:00:05Z",
	})
	if err != nil {
		t.Fatalf("finalize provider completion settlement idempotent retry: %v", err)
	}
	if idempotentSettlement.SettlementID != settlement.SettlementID {
		t.Fatalf("expected idempotent settlement retry to return original settlement, got %#v", idempotentSettlement)
	}

	attempt, found, err := s.GetAttempt(ctx, attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected attempt to exist")
	}
	if attempt.CanonicalSettlementID != "settlement_success_1" {
		t.Fatalf("expected canonical settlement to be linked on the attempt, got %q", attempt.CanonicalSettlementID)
	}

	invocation, found, err := s.GetProviderInvocation(ctx, "invocation_success_1")
	if err != nil {
		t.Fatalf("get provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider invocation to exist")
	}
	if invocation.InvocationState != ProviderInvocationStateSettledSuccess {
		t.Fatalf("expected settled_success invocation state, got %q", invocation.InvocationState)
	}

	persistedSettlement, found, err := s.GetProviderCompletionSettlementByInvocation(ctx, "invocation_success_1")
	if err != nil {
		t.Fatalf("get persisted provider completion settlement: %v", err)
	}
	if !found {
		t.Fatal("expected persisted provider completion settlement to exist")
	}
	if persistedSettlement.SettlementID != "settlement_success_1" {
		t.Fatalf("expected persisted settlement id, got %q", persistedSettlement.SettlementID)
	}
}

func TestStoreFinalizeProviderCompletionSettlementFailureFromDiagnosticEvidence(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_failure_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-failure-1",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_failure_1",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_failure_1",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	evidence, err := s.AppendProviderCompletionEvidence(ctx, AppendProviderCompletionEvidenceRequest{
		EvidenceID:           "stream_failure_evidence_1",
		ProviderInvocationID: "invocation_failure_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		EvidenceKind:         ProviderCompletionEvidenceKindStreamObservation,
		PayloadJSON:          `{"reason":"clean_exit_without_receipt"}`,
		CreatedAt:            "2026-03-01T12:00:04Z",
	})
	if err != nil {
		t.Fatalf("append diagnostic evidence: %v", err)
	}

	settlement, err := s.FinalizeProviderCompletionSettlement(ctx, FinalizeProviderCompletionSettlementRequest{
		SettlementID:         "settlement_failure_1",
		ProviderInvocationID: "invocation_failure_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		Outcome:              ProviderCompletionOutcomeFailed,
		PrimaryEvidenceID:    evidence.EvidenceID,
		FailureReasonCode:    "provider_incomplete_turn",
		FailureReasonText:    "clean provider exit without a valid completion receipt",
		SettledAt:            "2026-03-01T12:00:05Z",
	})
	if err != nil {
		t.Fatalf("finalize failed provider completion settlement: %v", err)
	}
	if settlement.Outcome != ProviderCompletionOutcomeFailed {
		t.Fatalf("expected failed settlement outcome, got %q", settlement.Outcome)
	}
	if settlement.FailureReasonCode != "provider_incomplete_turn" {
		t.Fatalf("expected failure reason code to persist, got %q", settlement.FailureReasonCode)
	}

	invocation, found, err := s.GetProviderInvocation(ctx, "invocation_failure_1")
	if err != nil {
		t.Fatalf("get provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider invocation to exist")
	}
	if invocation.InvocationState != ProviderInvocationStateSettledFailed {
		t.Fatalf("expected settled_failed invocation state, got %q", invocation.InvocationState)
	}
	if invocation.FailureReasonCode != "provider_incomplete_turn" {
		t.Fatalf("expected invocation failure reason code to persist, got %q", invocation.FailureReasonCode)
	}

	attempt, found, err := s.GetAttempt(ctx, attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected attempt to exist")
	}
	if attempt.CanonicalSettlementID != "settlement_failure_1" {
		t.Fatalf("expected failed settlement to be linked canonically on active attempt, got %q", attempt.CanonicalSettlementID)
	}
}

func TestStoreReconcileProviderCompletionSettlementClearsStaleSuccessPayloadWhenDowngraded(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_reconcile_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-reconcile-1",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_reconcile_1",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_reconcile_1",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	receipt, err := s.AcceptProviderCompletionReceipt(ctx, AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_reconcile_1",
		ProviderInvocationID:  "invocation_reconcile_1",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_1",
		ProtocolVersion:       "v1",
		CompletionCapability:  "capability-reconcile-1",
		FinalText:             "successful output that should not survive cancellation",
		StructuredPayloadJSON: `{"verdict":"PASS"}`,
		ReceivedAt:            "2026-03-01T12:00:04Z",
	})
	if err != nil {
		t.Fatalf("accept provider completion receipt: %v", err)
	}
	if _, err := s.FinalizeProviderCompletionSettlement(ctx, FinalizeProviderCompletionSettlementRequest{
		SettlementID:         "settlement_reconcile_1",
		ProviderInvocationID: "invocation_reconcile_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		Outcome:              ProviderCompletionOutcomeSuccess,
		PrimaryEvidenceID:    receipt.Evidence.EvidenceID,
		SettledAt:            "2026-03-01T12:00:05Z",
	}); err != nil {
		t.Fatalf("finalize provider completion settlement: %v", err)
	}

	settlement, err := s.ReconcileProviderCompletionSettlement(ctx, ReconcileProviderCompletionSettlementRequest{
		ProviderInvocationID: "invocation_reconcile_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		Outcome:              ProviderCompletionOutcomeCancelled,
		FailureReasonCode:    "run_terminalized",
		FailureReasonText:    "run terminalized before projection",
		SettledAt:            "2026-03-01T12:00:06Z",
	})
	if err != nil {
		t.Fatalf("reconcile provider completion settlement: %v", err)
	}
	if settlement.Outcome != ProviderCompletionOutcomeCancelled {
		t.Fatalf("expected cancelled settlement outcome, got %q", settlement.Outcome)
	}
	if settlement.FinalText != "" {
		t.Fatalf("expected stale success payload to be cleared, got %q", settlement.FinalText)
	}
	if settlement.StructuredPayloadJSON != "{}" {
		t.Fatalf("expected structured payload to be cleared with the downgraded settlement, got %q", settlement.StructuredPayloadJSON)
	}
	if settlement.FailureReasonCode != "run_terminalized" {
		t.Fatalf("expected reconciled failure reason code, got %q", settlement.FailureReasonCode)
	}
}

func TestStoreReconcileProviderCompletionSettlementPreservesReceiptPayloadForStableFailedOutcome(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_reconcile_failed_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_decree_intake_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-reconcile-failed-1",
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_reconcile_failed_1",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_reconcile_failed_1",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	receipt, err := s.AcceptProviderCompletionReceipt(ctx, AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_reconcile_failed_1",
		ProviderInvocationID:  "invocation_reconcile_failed_1",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_1",
		ProtocolVersion:       "v1",
		CompletionCapability:  "capability-reconcile-failed-1",
		FinalText:             "VERDICT: REWORK\nReceipt-backed rework response.",
		StructuredPayloadJSON: `{"verdict":"REWORK"}`,
		ReceivedAt:            "2026-03-01T12:00:04Z",
	})
	if err != nil {
		t.Fatalf("accept provider completion receipt: %v", err)
	}
	if _, err := s.FinalizeProviderCompletionSettlement(ctx, FinalizeProviderCompletionSettlementRequest{
		SettlementID:          "settlement_reconcile_failed_1",
		ProviderInvocationID:  "invocation_reconcile_failed_1",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_1",
		Outcome:               ProviderCompletionOutcomeFailed,
		PrimaryEvidenceID:     receipt.Evidence.EvidenceID,
		FinalText:             "VERDICT: REWORK\nReceipt-backed rework response.",
		StructuredPayloadJSON: `{"verdict":"REWORK"}`,
		FailureReasonCode:     "execution_review_rework",
		FailureReasonText:     "review requested rework",
		SettledAt:             "2026-03-01T12:00:05Z",
	}); err != nil {
		t.Fatalf("finalize failed provider completion settlement: %v", err)
	}

	settlement, err := s.ReconcileProviderCompletionSettlement(ctx, ReconcileProviderCompletionSettlementRequest{
		ProviderInvocationID: "invocation_reconcile_failed_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		Outcome:              ProviderCompletionOutcomeFailed,
		FailureReasonCode:    "execution_review_rework",
		FailureReasonText:    "review still requests rework after durable reconcile",
		SettledAt:            "2026-03-01T12:00:06Z",
	})
	if err != nil {
		t.Fatalf("reconcile failed provider completion settlement: %v", err)
	}
	if settlement.FinalText != "VERDICT: REWORK\nReceipt-backed rework response." {
		t.Fatalf("expected receipt-backed failed payload to survive reconcile, got %q", settlement.FinalText)
	}
	if settlement.StructuredPayloadJSON != `{"verdict":"REWORK"}` {
		t.Fatalf("expected receipt-backed failed structured payload to survive reconcile, got %q", settlement.StructuredPayloadJSON)
	}
	if settlement.FailureReasonText != "review still requests rework after durable reconcile" {
		t.Fatalf("expected reconciled failure text to persist, got %q", settlement.FailureReasonText)
	}
}
