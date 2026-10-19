package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/testutil"
)

func TestProviderContentHandoffImportIsAtomicVerifiedAndLeaseFenced(t *testing.T) {
	for _, failure := range []string{"hash", "lease", "lineage", "none"} {
		t.Run(failure, func(t *testing.T) {
			s, db, settlement := phase2ProviderSettlementFixture(t)
			req := terminalHandoffRequest(settlement)
			text := strings.Repeat("canonical provider bytes\n", 10000)
			ref := providerexecution.ReferenceText(req.ProviderInvocationID, text)
			req.ContentReferences = []providerexecution.ContentReference{ref, ref}
			req.StreamObservation.CanonicalTerminalTextRef = &ref
			req.StreamObservation.CanonicalTerminalText = ""
			calls := 0
			req.CopyContent = func(_ providerexecution.ContentReference, w io.Writer) error {
				calls++
				if failure == "hash" {
					_, err := io.WriteString(w, strings.Repeat("x", len(text)))
					return err
				}
				_, err := io.WriteString(w, text)
				return err
			}
			if failure == "lease" {
				req.ExpectedLeaseVersion++
			}
			if failure == "lineage" {
				req.ContentReferences[0].InvocationID = "foreign"
			}
			result, err := s.PersistProviderTerminalHandoff(context.Background(), req)
			if failure != "none" {
				if err == nil {
					t.Fatal("accepted invalid content handoff")
				}
				for _, table := range []string{"provider_contents", "provider_content_chunks", "provider_terminal_handoffs"} {
					if countPhase2Table(t, db, table) != 0 {
						t.Fatalf("partial %s survived rollback", table)
					}
				}
				if failure == "lease" && calls != 0 {
					t.Fatal("read content before checking lease fence")
				}
				return
			}
			if err != nil || result.Idempotent {
				t.Fatalf("handoff: %v", err)
			}
			if calls != 1 || countPhase2Table(t, db, "provider_contents") != 1 {
				t.Fatal("duplicated shared content")
			}
			var b strings.Builder
			if err := s.CopyProviderContent(context.Background(), ref, &b); err != nil || b.String() != text {
				t.Fatalf("canonical content: %v", err)
			}
			if replay, err := s.PersistProviderTerminalHandoff(context.Background(), req); err != nil || !replay.Idempotent {
				t.Fatalf("replay: %v", err)
			}
			if countPhase2Table(t, db, "provider_completion_settlements") != 0 {
				t.Fatal("handoff authorized success without settlement")
			}
			if _, err := db.Exec(`UPDATE provider_content_chunks SET content = X'00'`); err == nil {
				t.Fatal("canonical content allowed mutation")
			}
		})
	}
}

func TestSettlementChecksEveryDurableContinuationGuardBeyondThree(t *testing.T) {
	s, db, req := providerProcessGuardFixture(t)
	var first AcknowledgeProviderProcessGuardRequest
	for ordinal := int64(1); ordinal <= 7; ordinal++ {
		ack := providerProcessGuardAcknowledgement(t, s, req, fmt.Sprintf("continuation-guard-%d", ordinal), ordinal)
		if _, err := s.AcknowledgeProviderProcessGuard(context.Background(), ack); err != nil {
			t.Fatal(err)
		}
		if ordinal == 1 {
			first = ack
			continue
		}
		if _, err := s.IngestProviderProcessGuardQuiescence(context.Background(), providerProcessGuardQuiescenceProof(ack)); err != nil {
			t.Fatal(err)
		}
		completeBoundCommand(t, s, ack.ProviderInvocationID, ack.GuardID, ordinal)
	}
	finishBoundRequest(t, s, &req)
	handoff, err := s.PersistProviderTerminalHandoff(context.Background(), terminalHandoffRequest(req))
	if err != nil {
		t.Fatal(err)
	}
	req.TerminalHandoffID = handoff.Handoff.HandoffID
	if _, err := s.SettleProviderCompletionAndTerminal(context.Background(), req); !errors.Is(err, providerrequest.ErrResourcesOpen) {
		t.Fatalf("last guard hid an earlier live command: %v", err)
	}
	if countPhase2Table(t, db, "provider_completion_settlements") != 0 {
		t.Fatal("unquiescent lineage partially settled")
	}
	if _, err := s.IngestProviderProcessGuardQuiescence(context.Background(), providerProcessGuardQuiescenceProof(first)); err != nil {
		t.Fatal(err)
	}
	completeBoundCommand(t, s, first.ProviderInvocationID, first.GuardID, 1)
	if _, err := s.SettleProviderCompletionAndTerminal(context.Background(), req); err != nil {
		t.Fatalf("complete seven-command lineage did not settle: %v", err)
	}
}

func TestSettlementRejectsEarlierGuardLeaseExitAndOrdinalConflicts(t *testing.T) {
	for _, mutation := range []string{"lease_version=lease_version+1", "exit_code=2", "command_ordinal=8"} {
		t.Run(mutation, func(t *testing.T) {
			s, db, req := providerProcessGuardFixture(t)
			for ordinal := int64(1); ordinal <= 7; ordinal++ {
				ack := providerProcessGuardAcknowledgement(t, s, req, fmt.Sprintf("guard-%d", ordinal), ordinal)
				if _, err := s.AcknowledgeProviderProcessGuard(context.Background(), ack); err != nil {
					t.Fatal(err)
				}
				if _, err := s.IngestProviderProcessGuardQuiescence(context.Background(), providerProcessGuardQuiescenceProof(ack)); err != nil {
					t.Fatal(err)
				}
				completeBoundCommand(t, s, ack.ProviderInvocationID, ack.GuardID, ordinal)
			}
			finishBoundRequest(t, s, &req)
			handoff, err := s.PersistProviderTerminalHandoff(context.Background(), terminalHandoffRequest(req))
			if err != nil {
				t.Fatal(err)
			}
			req.TerminalHandoffID = handoff.Handoff.HandoffID
			mustExec(t, db, `UPDATE provider_process_guards SET `+mutation+` WHERE guard_id='guard-1'`)
			if _, err := s.SettleProviderCompletionAndTerminal(context.Background(), req); err == nil {
				t.Fatal("settled using only the last clean guard")
			}
			if countPhase2Table(t, db, "provider_completion_settlements") != 0 {
				t.Fatal("invalid guard lineage partially settled")
			}
		})
	}
}

func TestReferencedSettlementProjectionUsesVerifiedVerdictBeyondPreview(t *testing.T) {
	s, db, req := unfinishedPhase2ProviderSettlementFixture(t)
	mustExec(t, db, `UPDATE workflow_nodes SET node_class='execution_review'`)
	body := strings.TrimSpace("VERDICT:" + strings.Repeat(" ", 64*1024) + "PASS\n" + strings.Repeat("full response\n", 400000))
	ref := providerexecution.ReferenceText(req.Settlement.ProviderInvocationID, body)
	req.StreamObservation.CanonicalTerminalText = ""
	req.StreamObservation.CanonicalTerminalTextRef = &ref
	req.StreamObservation.Request.Terminal.Text = &ref
	finishBoundRequest(t, s, &req)
	handoffReq := terminalHandoffRequest(req)
	handoffReq.ContentReferences = []providerexecution.ContentReference{ref}
	handoffReq.CopyContent = func(_ providerexecution.ContentReference, w io.Writer) error {
		_, err := io.WriteString(w, body)
		return err
	}
	handoff, err := s.PersistProviderTerminalHandoff(context.Background(), handoffReq)
	if err != nil {
		t.Fatal(err)
	}
	req.TerminalHandoffID = handoff.Handoff.HandoffID
	settled, err := s.SettleProviderCompletionAndTerminal(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if settled.FinalText != "" || settled.FinalTextRef == nil || len(settled.FinalTextPreview) > 32*1024+4 {
		t.Fatal("canonical settlement materialized the answer")
	}
	detail, found, err := s.GetRunExecutionDetail(context.Background(), req.Settlement.RunID, req.Settlement.AttemptID)
	if err != nil || !found || detail.ObservedOutputTextRef == nil || detail.ObservedControlVerdict.String != "PASS" {
		t.Fatalf("detail lost canonical content/verdict: %+v err=%v", detail, err)
	}
	if strings.Contains(detail.ObservedOutputText.String, "PASS") {
		t.Fatal("fixture did not put verdict beyond display preview")
	}
	outputs, err := s.ListLatestRunExecutionOutputs(context.Background(), ListRunExecutionOutputsRequest{RunID: req.Settlement.RunID, States: []string{"committed_success"}})
	if err != nil || len(outputs) != 1 || outputs[0].OutputTextRef == nil {
		t.Fatalf("outputs=%+v err=%v", outputs, err)
	}
	if err := s.CopyProviderContent(context.Background(), *outputs[0].OutputTextRef, io.Discard); err != nil {
		t.Fatal(err)
	}
}

func TestProviderStreamContentBaselinePreservesSettledBytes(t *testing.T) {
	s, db, req := unfinishedPhase2ProviderSettlementFixture(t)
	ctx := context.Background()
	body := strings.Repeat("complete response 界", 100000)
	ref := providerexecution.ReferenceText(req.Settlement.ProviderInvocationID, body)
	req.StreamObservation.CanonicalTerminalText = ""
	req.StreamObservation.CanonicalTerminalTextRef = &ref
	req.StreamObservation.Request.Terminal.Text = &ref
	finishBoundRequest(t, s, &req)
	handoffReq := terminalHandoffRequest(req)
	handoffReq.ContentReferences = []providerexecution.ContentReference{ref}
	handoffReq.CopyContent = func(_ providerexecution.ContentReference, w io.Writer) error {
		_, err := io.WriteString(w, body)
		return err
	}
	handoff, err := s.PersistProviderTerminalHandoff(ctx, handoffReq)
	if err != nil {
		t.Fatal(err)
	}
	req.TerminalHandoffID = handoff.Handoff.HandoffID
	if _, err := s.SettleProviderCompletionAndTerminal(ctx, req); err != nil {
		t.Fatal(err)
	}
	settled, found, err := s.GetProviderCompletionSettlement(ctx, req.Settlement.SettlementID)
	if err != nil || !found || settled.FinalTextRef == nil || *settled.FinalTextRef != ref || settled.FinalText != "" {
		t.Fatalf("settlement lost canonical reference: %+v found=%t err=%v", settled, found, err)
	}
	var copied strings.Builder
	if err := s.CopyProviderContent(ctx, ref, &copied); err != nil || copied.String() != body {
		t.Fatalf("settled content lost bytes: %v", err)
	}
	for _, mutation := range []string{
		`UPDATE provider_contents SET size_bytes=0`,
		`UPDATE provider_content_chunks SET content=X'00'`,
	} {
		if _, err := db.Exec(mutation); err == nil {
			t.Fatalf("immutable provider content accepted %s", mutation)
		}
	}
}

func TestStoreActivateProviderInvocationOpensAndLinksAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:work_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:         attemptID,
		RunID:             "run_1",
		NodeRunID:         "nr_1",
		ExecutionMode:     ExecutionModeProviderBacked,
		CompletionMode:    ProviderCompletionModeServerResponse,
		SessionRef:        "session_run_1_work_intake_1",
		ProviderSessionID: "",
		CreatedAt:         "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}

	req := ActivateProviderInvocationRequest{
		ProviderInvocationID: "invocation_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_work_intake_1",
		ProviderSessionID:    "ses_live_1",
		CompletionMode:       ProviderCompletionModeServerResponse,
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}
	opened, err := activateProviderInvocationForTest(t, s, ctx, req)
	if err != nil {
		t.Fatalf("activate provider invocation: %v", err)
	}
	if opened.InvocationState != ProviderInvocationStateOpen {
		t.Fatalf("expected open invocation state, got %q", opened.InvocationState)
	}
	if opened.CompletionMode != ProviderCompletionModeServerResponse {
		t.Fatalf("expected completion_mode=%q, got %q", ProviderCompletionModeServerResponse, opened.CompletionMode)
	}
	if opened.ProviderSessionID != "ses_live_1" {
		t.Fatalf("expected provider session id to persist, got %q", opened.ProviderSessionID)
	}
	if opened.OpenedAt != "2026-03-01T12:00:03Z" {
		t.Fatalf("expected opened_at to persist, got %q", opened.OpenedAt)
	}

	replayed, err := activateProviderInvocationForTest(t, s, ctx, req)
	if err != nil {
		t.Fatalf("activate provider invocation idempotent retry: %v", err)
	}
	if replayed != opened {
		t.Fatalf("expected same invocation on idempotent activation, got %#v", replayed)
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

func TestStoreActivateProviderInvocationRejectsAttemptLineageMismatch(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version) VALUES ('nr_2', 'run_1', 'completion', 1, 1, 'running', 'governance', 'agent_governance', 1)`)
	s := New(db)
	const attemptID = "run_1:lineage:1"
	if err := s.EnqueueAttempt(context.Background(), EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: ExecutionModeProviderBacked, CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:01Z"}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}

	_, err := activateProviderInvocationForTest(t, s, context.Background(), ActivateProviderInvocationRequest{ProviderInvocationID: "invocation_wrong_lineage", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_2", LogicalSessionID: "logical_lineage", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:02Z"})
	if err == nil || !strings.Contains(err.Error(), "lineage") {
		t.Fatalf("expected attempt lineage rejection, got %v", err)
	}
	if count := countPhase2Table(t, db, "provider_invocations"); count != 0 {
		t.Fatalf("lineage rejection persisted %d provider invocations", count)
	}
	if _, err := db.Exec(`INSERT INTO provider_invocations (provider_invocation_id, attempt_id, run_id, node_run_id, invocation_state, completion_mode, created_at, updated_at) VALUES ('invocation_wrong_lineage_sql', ?, 'run_1', 'nr_2', 'issued', 'server_response', '2026-03-01T12:00:02Z', '2026-03-01T12:00:02Z')`, attemptID); err == nil {
		t.Fatal("expected SQL constraints to reject cross-lineage provider invocation")
	}
	if _, err := activateProviderInvocationForTest(t, s, context.Background(), ActivateProviderInvocationRequest{ProviderInvocationID: "invocation_valid_lineage", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical_lineage", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:02Z"}); err != nil {
		t.Fatalf("activate valid provider invocation: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO token_usage (usage_id, run_id, node_run_id, attempt_id, provider_invocation_id, required_role, created_at) VALUES ('usage_wrong_lineage', 'run_1', 'nr_2', ?, 'invocation_valid_lineage', 'governance', '2026-03-01T12:00:03Z')`, attemptID); err == nil {
		t.Fatal("expected SQL constraints to reject cross-lineage provider token usage")
	}
}

func TestStoreSettleProviderCompletionAndTerminalCommitsOneCanonicalBoundary(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	clockNow := time.Date(2026, 3, 1, 12, 0, 4, 0, time.UTC)
	s.now = func() time.Time { return clockNow }
	ctx := context.Background()
	const attemptID = "run_1:atomic_settlement:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: ExecutionModeProviderBacked, CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:01Z"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if acquired, err := s.AcquireLease(ctx, AcquireLeaseRequest{AttemptID: attemptID, LeaseOwner: "worker_atomic", LeaseUntil: "2026-03-01T12:10:00Z", AcquiredAt: "2026-03-01T12:00:03Z", StateOnLease: "running"}); err != nil || !acquired {
		t.Fatalf("acquire settlement lease: acquired=%t err=%v", acquired, err)
	}
	if _, err := activateProviderInvocationForTest(t, s, ctx, ActivateProviderInvocationRequest{ProviderInvocationID: "invocation_atomic_1", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical_atomic", ProviderSessionID: "ses_atomic", CompletionMode: ProviderCompletionModeServerResponse, IssuedAt: "2026-03-01T12:00:02Z", CreatedAt: "2026-03-01T12:00:02Z", OpenedAt: "2026-03-01T12:00:03Z"}); err != nil {
		t.Fatalf("activate invocation: %v", err)
	}
	observation := boundSuccessObservation("invocation_atomic_1", "ses_atomic", "canonical response")
	observation.Tokens = providerexecution.TokenUsage{InputTokens: 10, OutputTokens: 4, TotalTokens: 14, ReasoningTokens: 2}
	settleReq := SettleProviderCompletionAndTerminalRequest{
		Settlement:           FinalizeProviderCompletionSettlementRequest{SettlementID: "settlement_atomic_1", ProviderInvocationID: "invocation_atomic_1", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", Outcome: ProviderCompletionOutcomeSuccess, FinalText: "canonical response", StructuredPayloadJSON: `{}`, ProviderDiagnosticsJSON: `{}`},
		StreamObservation:    &observation,
		ExpectedLeaseOwner:   "worker_atomic",
		ExpectedLeaseVersion: 1,
		TokenUsage:           &ProviderInvocationTokenUsage{RequiredRole: "governance", Model: "github-copilot/gpt-5.4-mini", Variant: "low"},
		Terminal:             CommitTerminalRequest{OutboxID: "outbox_atomic_1", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", TerminalState: "committed_success", PayloadJSON: `{"output_text":"canonical response"}`, ProviderSessionID: "ses_atomic"},
	}
	finishBoundRequest(t, s, &settleReq)
	attachTerminalHandoffToSettlement(t, s, &settleReq)
	settlement, err := s.SettleProviderCompletionAndTerminal(ctx, settleReq)
	if err != nil {
		t.Fatalf("atomic settlement: %v", err)
	}
	if settlement.PrimaryEvidenceID != TerminalProviderCompletionEvidenceID("invocation_atomic_1") {
		t.Fatalf("unexpected primary evidence: %#v", settlement)
	}
	evidence, err := s.ListProviderCompletionEvidence(ctx, "invocation_atomic_1")
	if err != nil || len(evidence) != 1 {
		t.Fatalf("load durable stream evidence: evidence=%#v err=%v", evidence, err)
	}
	if evidence[0].EvidenceID != settlement.PrimaryEvidenceID || evidence[0].EvidenceKind != ProviderCompletionEvidenceKindStreamObservation {
		t.Fatalf("terminal settlement did not use durable stream observation as primary evidence: settlement=%#v evidence=%#v", settlement, evidence)
	}
	if settlement.SettledAt != "2026-03-01T12:00:04Z" || settlement.CreatedAt != settlement.SettledAt {
		t.Fatalf("expected store-owned canonical settlement timestamp, got %#v", settlement)
	}
	attempt, found, err := s.GetAttempt(ctx, attemptID)
	if err != nil || !found {
		t.Fatalf("load attempt: found=%t err=%v", found, err)
	}
	if attempt.State != "committed_success" || attempt.CanonicalSettlementID != settlement.SettlementID {
		t.Fatalf("expected committed settled attempt, got %#v", attempt)
	}
	if attempt.LeaseOwner != "" || attempt.LeaseExpiresAt != "" || attempt.LeaseVersion != 1 {
		t.Fatalf("terminal settlement did not clear the lease while retaining its fence: %#v", attempt)
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
	var usageInvocationID string
	var totalTokens int64
	if err := db.QueryRow(`SELECT provider_invocation_id, total_tokens FROM token_usage WHERE provider_invocation_id = 'invocation_atomic_1'`).Scan(&usageInvocationID, &totalTokens); err != nil {
		t.Fatalf("load invocation usage: %v", err)
	}
	if usageInvocationID != "invocation_atomic_1" || totalTokens != 14 {
		t.Fatalf("unexpected invocation usage: invocation=%q total=%d", usageInvocationID, totalTokens)
	}
	clockNow = clockNow.Add(time.Second)
	if replayed, err := s.SettleProviderCompletionAndTerminal(ctx, settleReq); err != nil || replayed != settlement {
		t.Fatalf("timestamp-omitting settlement replay in a later second did not converge: settlement=%#v err=%v", replayed, err)
	}
	replayWithLaterCallerTime := settleReq
	replayWithLaterCallerTime.Settlement.SettledAt = clockNow.Format(time.RFC3339Nano)
	replayWithLaterCallerTime.Terminal.CommittedAt = clockNow.Format(time.RFC3339Nano)
	replayWithLaterCallerTime.TokenUsage = &ProviderInvocationTokenUsage{
		RequiredRole: "governance",
		Model:        "github-copilot/gpt-5.4-mini",
		Variant:      "low",
		CreatedAt:    clockNow.Format(time.RFC3339Nano),
	}
	if replayed, err := s.SettleProviderCompletionAndTerminal(ctx, replayWithLaterCallerTime); err != nil || replayed != settlement {
		t.Fatalf("settlement replay with a later caller wall-clock timestamp did not converge: settlement=%#v err=%v", replayed, err)
	}
	for label, query := range map[string]string{
		"invocation":                   `SELECT settled_at FROM provider_invocations WHERE provider_invocation_id = 'invocation_atomic_1'`,
		"evidence":                     `SELECT created_at FROM provider_completion_evidence WHERE evidence_id = 'evidence_terminal_invocation_atomic_1'`,
		"attempt terminal observation": `SELECT terminal_observed_at FROM attempt_executions WHERE attempt_id = 'run_1:atomic_settlement:1'`,
		"attempt commit":               `SELECT committed_at FROM attempt_executions WHERE attempt_id = 'run_1:atomic_settlement:1'`,
		"attempt event":                `SELECT created_at FROM attempt_event_log WHERE attempt_id = 'run_1:atomic_settlement:1'`,
		"outbox creation":              `SELECT created_at FROM attempt_outbox WHERE outbox_id = 'outbox_atomic_1'`,
		"outbox update":                `SELECT updated_at FROM attempt_outbox WHERE outbox_id = 'outbox_atomic_1'`,
		"token usage":                  `SELECT created_at FROM token_usage WHERE provider_invocation_id = 'invocation_atomic_1'`,
	} {
		var timestamp string
		if err := db.QueryRow(query).Scan(&timestamp); err != nil {
			t.Fatalf("load %s timestamp: %v", label, err)
		}
		if timestamp != settlement.SettledAt {
			t.Fatalf("expected %s timestamp %q to use canonical settlement time %q", label, timestamp, settlement.SettledAt)
		}
	}
	conflictingObservation := observation
	conflictingObservation.Tokens.TotalTokens = 15
	conflictingUsage := settleReq
	conflictingUsage.StreamObservation = &conflictingObservation
	_, err = s.SettleProviderCompletionAndTerminal(ctx, conflictingUsage)
	var usageConflict *ProviderSettlementError
	if !errors.As(err, &usageConflict) || usageConflict.Reason != "evidence_conflict" {
		t.Fatalf("expected contradictory observation replay conflict, got %v", err)
	}
	conflictingTerminal := settleReq
	conflictingTerminal.Terminal.PayloadJSON = `{"output_text":"changed response"}`
	_, err = s.SettleProviderCompletionAndTerminal(ctx, conflictingTerminal)
	var terminalConflict *ProviderSettlementError
	if !errors.As(err, &terminalConflict) || terminalConflict.Reason != "terminal_event_replay_conflict" {
		t.Fatalf("expected contradictory terminal replay conflict, got %v", err)
	}
	conflictingTokenUsage := settleReq
	conflictingTokenUsage.TokenUsage = &ProviderInvocationTokenUsage{RequiredRole: "governance", Model: "changed-model", Variant: "low"}
	_, err = s.SettleProviderCompletionAndTerminal(ctx, conflictingTokenUsage)
	var tokenConflict *ProviderSettlementError
	if !errors.As(err, &tokenConflict) || tokenConflict.Reason != "token_usage_conflict" {
		t.Fatalf("expected contradictory token replay conflict, got %v", err)
	}
	if _, err := db.Exec(`INSERT INTO provider_invocations (provider_invocation_id, attempt_id, run_id, node_run_id, logical_session_id, invocation_state, completion_mode, created_at, updated_at) VALUES ('invocation_atomic_stale', ?, 'run_1', 'nr_1', 'logical_atomic', 'issued', 'server_response', '2026-03-01T12:00:05Z', '2026-03-01T12:00:05Z')`, attemptID); err != nil {
		t.Fatalf("insert stale invocation: %v", err)
	}
	_, err = s.SettleProviderCompletionAndTerminal(ctx, SettleProviderCompletionAndTerminalRequest{
		Settlement:        FinalizeProviderCompletionSettlementRequest{SettlementID: "settlement_atomic_stale", ProviderInvocationID: "invocation_atomic_stale", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", Outcome: ProviderCompletionOutcomeFailed, PrimaryEvidenceID: "unused", ProviderDiagnosticsJSON: `{}`, SettledAt: "2026-03-01T12:00:06Z"},
		StreamObservation: &observation,
		TokenUsage:        &ProviderInvocationTokenUsage{RequiredRole: "governance", CreatedAt: "2026-03-01T12:00:06Z"},
		Terminal:          CommitTerminalRequest{OutboxID: "outbox_atomic_stale", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", TerminalState: "committed_failure", PayloadJSON: `{}`, CommittedAt: "2026-03-01T12:00:06Z"},
	})
	var settlementErr *ProviderSettlementError
	if !errors.As(err, &settlementErr) || settlementErr.Reason != "invocation_not_active" {
		t.Fatalf("expected stale invocation to be rejected, got %v", err)
	}
}

func TestStoreLostHandoffSettlesExpiredInvocationOnlyAsFailure(t *testing.T) {
	t.Parallel()
	s, db, req := atomicProviderSettlementFixture(t)
	if req.TerminalHandoffID != "" || countPhase2Table(t, db, "provider_terminal_handoffs") != 0 {
		t.Fatal("lost handoff fixture unexpectedly has a durable terminal handoff")
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET lease_expires_at = '2026-03-01T11:59:00Z' WHERE attempt_id = ?`, req.Settlement.AttemptID); err != nil {
		t.Fatal(err)
	}
	req.LostHandoff = true
	req.Settlement.Outcome = ProviderCompletionOutcomeFailed
	req.Settlement.FinalText = ""
	req.Settlement.FailureReasonCode = "provider_terminal_handoff_lost"
	req.Settlement.FailureReasonText = "Provider execution ended, but its terminal observation was lost before durable handoff."
	req.Terminal.TerminalState = "committed_failure"
	req.Terminal.PayloadJSON = `{"status":"failed","error_reason":"provider_terminal_handoff_lost"}`
	req.Terminal.FailureCode = req.Settlement.FailureReasonCode
	req.Terminal.FailureText = req.Settlement.FailureReasonText
	req.Terminal.TerminalReasonCode = req.Settlement.FailureReasonCode
	if _, err := s.SettleProviderCompletionAndTerminal(context.Background(), req); err != nil {
		t.Fatalf("settle recovered quiescence failure: %v", err)
	}
	if count := countPhase2Table(t, db, "provider_terminal_handoffs"); count != 0 {
		t.Fatalf("recovered guard failure created %d terminal handoffs", count)
	}

	s2, db2, success := atomicProviderSettlementFixture(t)
	if _, err := db2.Exec(`UPDATE attempt_executions SET lease_expires_at = '2026-03-01T11:59:00Z' WHERE attempt_id = ?`, success.Settlement.AttemptID); err != nil {
		t.Fatal(err)
	}
	success.LostHandoff = true
	if _, err := s2.SettleProviderCompletionAndTerminal(context.Background(), success); err == nil {
		t.Fatal("recovered guard quiescence authorized success")
	} else {
		requireProviderSettlementError(t, err, "settle_provider_completion_and_terminal", "lost_handoff_cannot_succeed")
	}
}

func TestStoreSettleProviderCompletionAndTerminalRejectsInvalidLeaseWithoutPartialWrites(t *testing.T) {
	tests := []struct {
		name    string
		owner   string
		version int64
	}{
		{name: "blank owner", owner: "", version: 1},
		{name: "zero version", owner: "worker-a", version: 0},
		{name: "stale owner", owner: "worker-b", version: 1},
		{name: "stale version", owner: "worker-a", version: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, db, req := atomicProviderSettlementFixture(t)
			attachTerminalHandoffToSettlement(t, s, &req)
			req.ExpectedLeaseOwner = tc.owner
			req.ExpectedLeaseVersion = tc.version
			_, err := s.SettleProviderCompletionAndTerminal(context.Background(), req)
			if err == nil || !strings.Contains(err.Error(), "bound identity and lease") {
				t.Fatalf("expected bound lease rejection, got %v", err)
			}

			for table := range map[string]struct{}{
				"provider_completion_settlements": {},
				"provider_completion_evidence":    {},
				"attempt_event_log":               {},
				"attempt_outbox":                  {},
				"token_usage":                     {},
			} {
				var count int
				if err := db.QueryRow(`SELECT COUNT(1) FROM ` + table).Scan(&count); err != nil {
					t.Fatalf("count %s: %v", table, err)
				}
				if count != 0 {
					t.Fatalf("lease rejection partially wrote %s: count=%d", table, count)
				}
			}
			if count := countPhase2Table(t, db, "provider_terminal_handoffs"); count != 1 {
				t.Fatalf("lease rejection did not preserve its durable handoff: count=%d", count)
			}
			attempt, found, err := s.GetAttempt(context.Background(), req.Settlement.AttemptID)
			if err != nil || !found {
				t.Fatalf("load rejected attempt: found=%t err=%v", found, err)
			}
			if attempt.State != "running" || attempt.CanonicalSettlementID != "" || attempt.LeaseOwner != "worker-a" || attempt.LeaseVersion != 1 {
				t.Fatalf("lease rejection changed canonical attempt state: %#v", attempt)
			}
		})
	}
}

func TestStoreSettleProviderCompletionAndTerminalRequiresQuiescentCleanGuards(t *testing.T) {
	acknowledge := func(t *testing.T, s *Store, req *SettleProviderCompletionAndTerminalRequest) {
		acknowledgeProviderProcessGuard(t, s, req.Settlement.ProviderInvocationID, req.Settlement.AttemptID, req.Settlement.RunID, req.Settlement.NodeRunID, req.ExpectedLeaseOwner, req.ExpectedLeaseVersion)
	}
	acknowledgeQuiescent := func(t *testing.T, s *Store, req *SettleProviderCompletionAndTerminalRequest) {
		acknowledgeQuiescentProviderProcessGuard(t, s, req.Settlement.ProviderInvocationID, req.Settlement.AttemptID, req.Settlement.RunID, req.Settlement.NodeRunID, req.ExpectedLeaseOwner, req.ExpectedLeaseVersion)
	}
	tests := []struct {
		name    string
		outcome ProviderCompletionOutcome
		prepare func(t *testing.T, s *Store, db *sql.DB, req *SettleProviderCompletionAndTerminalRequest)
		reason  string
	}{
		{
			name: "success nonquiescent guard",
			prepare: func(t *testing.T, s *Store, _ *sql.DB, req *SettleProviderCompletionAndTerminalRequest) {
				acknowledge(t, s, req)
			},
		},
		{
			name:    "failure nonquiescent launched guard",
			outcome: ProviderCompletionOutcomeFailed,
			prepare: func(t *testing.T, s *Store, _ *sql.DB, req *SettleProviderCompletionAndTerminalRequest) {
				acknowledge(t, s, req)
			},
		},
		{
			name: "success forced guard",
			prepare: func(t *testing.T, s *Store, db *sql.DB, req *SettleProviderCompletionAndTerminalRequest) {
				acknowledgeQuiescent(t, s, req)
				if _, err := db.Exec(`UPDATE provider_process_guards SET termination_forced = 1 WHERE provider_invocation_id = ?`, req.Settlement.ProviderInvocationID); err != nil {
					t.Fatalf("force fixture guard: %v", err)
				}
			},
			reason: "provider_process_guard_forced",
		},
		{
			name: "success unclean guard",
			prepare: func(t *testing.T, s *Store, db *sql.DB, req *SettleProviderCompletionAndTerminalRequest) {
				acknowledgeQuiescent(t, s, req)
				if _, err := db.Exec(`UPDATE provider_process_guards SET exit_code = 2 WHERE provider_invocation_id = ?`, req.Settlement.ProviderInvocationID); err != nil {
					t.Fatalf("fail fixture guard: %v", err)
				}
			},
			reason: "provider_process_guard_incomplete",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, db, req := unfinishedPhase2ProviderSettlementFixture(t)
			if tc.outcome != "" {
				req.Settlement.Outcome = tc.outcome
				req.Settlement.FailureReasonCode = "provider_failed"
				req.Settlement.FailureReasonText = "provider process failed"
				req.Terminal.TerminalState = "committed_failure"
				req.Terminal.FailureCode = "provider_failed"
				req.Terminal.FailureText = "provider process failed"
			}
			tc.prepare(t, s, db, &req)
			finishBoundRequest(t, s, &req)
			attachTerminalHandoffToSettlement(t, s, &req)
			_, err := s.SettleProviderCompletionAndTerminal(context.Background(), req)
			if tc.reason == "" {
				if !errors.Is(err, providerrequest.ErrResourcesOpen) {
					t.Fatalf("unresolved command guardian did not keep resources open: %v", err)
				}
			} else {
				requireProviderSettlementError(t, err, "settle_provider_completion_and_terminal", tc.reason)
			}
			if count := countPhase2Table(t, db, "provider_completion_settlements"); count != 0 {
				t.Fatalf("guard rejection persisted %d settlement rows", count)
			}
		})
	}
}

func TestStorePersistProviderTerminalHandoffRequiresObservedLeaseAndExactLineage(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *sql.DB, *PersistProviderTerminalHandoffRequest)
		reason string
		// message is the required error text of a rejection that has no
		// settlement reason because the request itself disagrees with its lease.
		message string
	}{
		{
			name: "observed after lease expiry",
			mutate: func(t *testing.T, db *sql.DB, req *PersistProviderTerminalHandoffRequest) {
				if _, err := db.Exec(`UPDATE attempt_executions SET lease_expires_at = '2026-03-01T12:00:03Z' WHERE attempt_id = ?`, req.AttemptID); err != nil {
					t.Fatalf("expire handoff lease: %v", err)
				}
			},
			reason: "handoff_created_after_lease_expiry",
		},
		{
			name: "observed at lease expiry",
			mutate: func(t *testing.T, db *sql.DB, req *PersistProviderTerminalHandoffRequest) {
				if _, err := db.Exec(`UPDATE attempt_executions SET lease_expires_at = '2026-03-01T12:00:03Z' WHERE attempt_id = ?`, req.AttemptID); err != nil {
					t.Fatalf("set handoff lease expiry: %v", err)
				}
				req.CreatedAt = "2026-03-01T12:00:03Z"
			},
			reason: "handoff_created_after_lease_expiry",
		},
		{
			name: "future observation",
			mutate: func(_ *testing.T, _ *sql.DB, req *PersistProviderTerminalHandoffRequest) {
				req.CreatedAt = "2026-03-01T12:00:05Z"
			},
			reason: "handoff_created_at_future",
		},
		{
			name: "invalid observation timestamp",
			mutate: func(_ *testing.T, _ *sql.DB, req *PersistProviderTerminalHandoffRequest) {
				req.CreatedAt = "not-a-time"
			},
			reason: "handoff_created_at_invalid",
		},
		{
			name: "lease owner mismatch",
			mutate: func(_ *testing.T, _ *sql.DB, req *PersistProviderTerminalHandoffRequest) {
				req.ExpectedLeaseOwner = "another-worker"
			},
			message: "exact bound observation and lease",
		},
		{
			name: "attempt lease reclaimed",
			mutate: func(t *testing.T, db *sql.DB, req *PersistProviderTerminalHandoffRequest) {
				if _, err := db.Exec(`UPDATE attempt_executions SET lease_owner = 'another-worker' WHERE attempt_id = ?`, req.AttemptID); err != nil {
					t.Fatalf("reclaim attempt lease: %v", err)
				}
			},
			reason: "lease_lineage_mismatch",
		},
		{
			name: "blank lease owner",
			mutate: func(_ *testing.T, _ *sql.DB, req *PersistProviderTerminalHandoffRequest) {
				req.ExpectedLeaseOwner = ""
			},
			message: "exact bound observation and lease",
		},
		{
			name: "nonpositive lease version",
			mutate: func(_ *testing.T, _ *sql.DB, req *PersistProviderTerminalHandoffRequest) {
				req.ExpectedLeaseVersion = 0
			},
			message: "exact bound observation and lease",
		},
		{
			name: "attempt lineage mismatch",
			mutate: func(_ *testing.T, _ *sql.DB, req *PersistProviderTerminalHandoffRequest) {
				req.AttemptID = "another-attempt"
			},
			reason: "attempt_lineage_mismatch",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, db, settlementReq := phase2ProviderSettlementFixture(t)
			req := terminalHandoffRequest(settlementReq)
			tc.mutate(t, db, &req)
			_, err := s.PersistProviderTerminalHandoff(context.Background(), req)
			if tc.message != "" {
				if err == nil || !strings.Contains(err.Error(), tc.message) {
					t.Fatalf("expected %q rejection, got %v", tc.message, err)
				}
			} else {
				requireProviderSettlementError(t, err, "persist_provider_terminal_handoff", tc.reason)
			}
			if count := countPhase2Table(t, db, "provider_terminal_handoffs"); count != 0 {
				t.Fatalf("rejected handoff persisted %d rows", count)
			}
		})
	}
}

func TestStoreMarksTerminalHandoffPendingBeforePublishingFile(t *testing.T) {
	t.Parallel()

	s, db, settlementReq := phase2ProviderSettlementFixture(t)
	mark := MarkProviderTerminalHandoffPendingRequest{
		ProviderInvocationID: settlementReq.Settlement.ProviderInvocationID,
		AttemptID:            settlementReq.Settlement.AttemptID,
		RunID:                settlementReq.Settlement.RunID,
		NodeRunID:            settlementReq.Settlement.NodeRunID,
		ExpectedLeaseOwner:   settlementReq.ExpectedLeaseOwner,
		ExpectedLeaseVersion: settlementReq.ExpectedLeaseVersion,
		MarkedAt:             "2026-03-01T12:00:03.500Z",
	}
	if err := s.MarkProviderTerminalHandoffPending(context.Background(), mark); err != nil {
		t.Fatalf("mark terminal handoff pending: %v", err)
	}
	if count := countPhase2Table(t, db, "provider_terminal_handoffs"); count != 0 {
		t.Fatalf("expected the marker to commit before the handoff row, got %d handoffs", count)
	}
	var markedAt sql.NullString
	var pendingLeaseOwner string
	var pendingLeaseVersion int64
	if err := db.QueryRow(`SELECT terminal_handoff_pending_at, terminal_handoff_pending_lease_owner, terminal_handoff_pending_lease_version FROM provider_invocations WHERE provider_invocation_id = ?`, mark.ProviderInvocationID).Scan(&markedAt, &pendingLeaseOwner, &pendingLeaseVersion); err != nil || !markedAt.Valid || markedAt.String != mark.MarkedAt || pendingLeaseOwner != mark.ExpectedLeaseOwner || pendingLeaseVersion != mark.ExpectedLeaseVersion {
		t.Fatalf("terminal handoff pending marker=%#v lease_owner=%q lease_version=%d err=%v", markedAt, pendingLeaseOwner, pendingLeaseVersion, err)
	}
	cleanupCandidates, err := s.ListProviderTerminalHandoffFileCleanupCandidates(context.Background(), "", 1)
	if err != nil || len(cleanupCandidates) != 0 {
		t.Fatalf("file cleanup must wait until the handoff row is durable: candidates=%#v err=%v", cleanupCandidates, err)
	}
	if err := s.MarkProviderTerminalHandoffPending(context.Background(), mark); err != nil {
		t.Fatalf("replay terminal handoff pending marker: %v", err)
	}
	if _, err := s.PersistProviderTerminalHandoff(context.Background(), terminalHandoffRequest(settlementReq)); err != nil {
		t.Fatalf("persist handoff after durable recovery marker: %v", err)
	}
	if count := countPhase2Table(t, db, "provider_terminal_handoffs"); count != 1 {
		t.Fatalf("handoff rows after publication=%d, want 1", count)
	}
	cleanupCandidates, err = s.ListProviderTerminalHandoffFileCleanupCandidates(context.Background(), "", 1)
	if err != nil || len(cleanupCandidates) != 1 {
		t.Fatalf("durable handoff should admit file cleanup replay: candidates=%#v err=%v", cleanupCandidates, err)
	}
}

func TestStoreMarkHandoffPendingConfirmsDurableCommitError(t *testing.T) {
	connector := testutil.NewSQLiteCommitFaultConnector(":memory:")
	db := connector.OpenDB()
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	s, db, settlementReq := phase2ProviderSettlementFixtureWithDB(t, db)
	mark := MarkProviderTerminalHandoffPendingRequest{
		ProviderInvocationID: settlementReq.Settlement.ProviderInvocationID,
		AttemptID:            settlementReq.Settlement.AttemptID,
		RunID:                settlementReq.Settlement.RunID,
		NodeRunID:            settlementReq.Settlement.NodeRunID,
		ExpectedLeaseOwner:   settlementReq.ExpectedLeaseOwner,
		ExpectedLeaseVersion: settlementReq.ExpectedLeaseVersion,
		MarkedAt:             "2026-03-01T12:00:03.500Z",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connector.FailNextDurableCommit(cancel)
	if err := s.MarkProviderTerminalHandoffPending(ctx, mark); err != nil {
		t.Fatalf("confirm durable pending marker after commit error: %v", err)
	}
	if ctx.Err() != context.Canceled {
		t.Fatal("expected simulated durable commit fault to cancel the request before readback")
	}
	var pendingAt sql.NullString
	if err := db.QueryRow(`SELECT terminal_handoff_pending_at FROM provider_invocations WHERE provider_invocation_id = ?`, mark.ProviderInvocationID).Scan(&pendingAt); err != nil || !pendingAt.Valid || pendingAt.String != mark.MarkedAt {
		t.Fatalf("durable pending marker readback=%#v err=%v", pendingAt, err)
	}
}

func TestStoreRejectsTerminalHandoffPendingMarkerForChangedLease(t *testing.T) {
	t.Parallel()

	s, db, settlementReq := phase2ProviderSettlementFixture(t)
	mark := MarkProviderTerminalHandoffPendingRequest{
		ProviderInvocationID: settlementReq.Settlement.ProviderInvocationID,
		AttemptID:            settlementReq.Settlement.AttemptID,
		RunID:                settlementReq.Settlement.RunID,
		NodeRunID:            settlementReq.Settlement.NodeRunID,
		ExpectedLeaseOwner:   "another-worker",
		ExpectedLeaseVersion: settlementReq.ExpectedLeaseVersion,
		MarkedAt:             "2026-03-01T12:00:03.500Z",
	}
	err := s.MarkProviderTerminalHandoffPending(context.Background(), mark)
	requireProviderSettlementError(t, err, "mark_provider_terminal_handoff_pending", "lease_lineage_mismatch")
	var markerCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM provider_invocations WHERE provider_invocation_id = ? AND terminal_handoff_pending_at IS NOT NULL`, mark.ProviderInvocationID).Scan(&markerCount); err != nil {
		t.Fatalf("count terminal handoff pending markers: %v", err)
	}
	if markerCount != 0 {
		t.Fatalf("rejected lease lineage persisted %d pending markers", markerCount)
	}
}

func TestStoreMarksConfirmedTerminalRecoverySeparatelyFromPendingHandoff(t *testing.T) {
	t.Parallel()

	s, db, req := phase2ProviderSettlementFixture(t)
	const markedAt = "2026-03-01T12:00:04Z"
	if err := s.MarkProviderTerminalRecoveryRequired(context.Background(), MarkProviderTerminalRecoveryRequiredRequest{
		ProviderInvocationID: req.Settlement.ProviderInvocationID,
		AttemptID:            req.Settlement.AttemptID,
		RunID:                req.Settlement.RunID,
		NodeRunID:            req.Settlement.NodeRunID,
		ExpectedLeaseOwner:   req.ExpectedLeaseOwner,
		ExpectedLeaseVersion: req.ExpectedLeaseVersion,
		MarkedAt:             markedAt,
	}); err != nil {
		t.Fatalf("mark confirmed terminal recovery required: %v", err)
	}
	var recoveryAt, pendingAt sql.NullString
	if err := db.QueryRow(`SELECT terminal_recovery_required_at, terminal_handoff_pending_at FROM provider_invocations WHERE provider_invocation_id = ?`, req.Settlement.ProviderInvocationID).Scan(&recoveryAt, &pendingAt); err != nil {
		t.Fatalf("load terminal recovery markers: %v", err)
	}
	if !recoveryAt.Valid || recoveryAt.String != markedAt || pendingAt.Valid {
		t.Fatalf("recovery-required and handoff-pending markers were conflated: recovery=%#v pending=%#v", recoveryAt, pendingAt)
	}
}

func TestStoreMarkRecoveryRequiredConfirmsDurableCommitError(t *testing.T) {
	connector := testutil.NewSQLiteCommitFaultConnector(":memory:")
	db := connector.OpenDB()
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	s, db, settlementReq := phase2ProviderSettlementFixtureWithDB(t, db)
	mark := MarkProviderTerminalRecoveryRequiredRequest{
		ProviderInvocationID: settlementReq.Settlement.ProviderInvocationID,
		AttemptID:            settlementReq.Settlement.AttemptID,
		RunID:                settlementReq.Settlement.RunID,
		NodeRunID:            settlementReq.Settlement.NodeRunID,
		ExpectedLeaseOwner:   settlementReq.ExpectedLeaseOwner,
		ExpectedLeaseVersion: settlementReq.ExpectedLeaseVersion,
		MarkedAt:             "2026-03-01T12:00:04Z",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connector.FailNextDurableCommit(cancel)
	if err := s.MarkProviderTerminalRecoveryRequired(ctx, mark); err != nil {
		t.Fatalf("confirm durable recovery marker after commit error: %v", err)
	}
	if ctx.Err() != context.Canceled {
		t.Fatal("expected simulated durable commit fault to cancel the request before readback")
	}
	var recoveryAt sql.NullString
	if err := db.QueryRow(`SELECT terminal_recovery_required_at FROM provider_invocations WHERE provider_invocation_id = ?`, mark.ProviderInvocationID).Scan(&recoveryAt); err != nil || !recoveryAt.Valid || recoveryAt.String != mark.MarkedAt {
		t.Fatalf("durable recovery marker readback=%#v err=%v", recoveryAt, err)
	}
}

func TestStoreHandoffFileCleanupMarkerUsesCompareAndClear(t *testing.T) {
	t.Parallel()

	s, db, req := phase2ProviderSettlementFixture(t)
	invocationID := req.Settlement.ProviderInvocationID
	if _, err := s.PersistProviderTerminalHandoff(context.Background(), terminalHandoffRequest(req)); err != nil {
		t.Fatalf("persist handoff row for cleanup marker test: %v", err)
	}
	if err := s.MarkProviderTerminalHandoffFileCleanupPending(context.Background(), invocationID, "2026-03-01T12:00:05Z"); err != nil {
		t.Fatalf("mark handoff file cleanup pending: %v", err)
	}
	if err := s.MarkProviderTerminalHandoffFileCleanupPending(context.Background(), invocationID, "2026-03-01T12:00:06Z"); err != nil {
		t.Fatalf("refresh handoff file cleanup marker: %v", err)
	}
	candidates, err := s.ListProviderTerminalHandoffFileCleanupCandidates(context.Background(), "", 1)
	if err != nil || len(candidates) != 1 || candidates[0].ProviderInvocationID != invocationID || candidates[0].MarkedAt != "2026-03-01T12:00:06Z" {
		t.Fatalf("cleanup candidates=%#v err=%v", candidates, err)
	}
	if err := s.ClearProviderTerminalHandoffFileCleanupPending(context.Background(), invocationID, "2026-03-01T12:00:05Z", "2026-03-01T12:00:07Z"); err != nil {
		t.Fatalf("clear stale handoff cleanup generation: %v", err)
	}
	candidates, err = s.ListProviderTerminalHandoffFileCleanupCandidates(context.Background(), "", 1)
	if err != nil || len(candidates) != 1 || candidates[0].MarkedAt != "2026-03-01T12:00:06Z" {
		t.Fatalf("stale cleanup generation cleared newer marker: candidates=%#v err=%v", candidates, err)
	}
	if err := s.ClearProviderTerminalHandoffFileCleanupPending(context.Background(), invocationID, candidates[0].MarkedAt, "2026-03-01T12:00:08Z"); err != nil {
		t.Fatalf("clear current handoff cleanup generation: %v", err)
	}
	candidates, err = s.ListProviderTerminalHandoffFileCleanupCandidates(context.Background(), "", 1)
	if err != nil || len(candidates) != 0 {
		t.Fatalf("cleanup candidates after clear=%#v err=%v", candidates, err)
	}
	var marker sql.NullString
	if err := db.QueryRow(`SELECT terminal_handoff_file_cleanup_pending_at FROM provider_invocations WHERE provider_invocation_id = ?`, invocationID).Scan(&marker); err != nil || marker.Valid {
		t.Fatalf("cleanup marker after clear=%#v err=%v", marker, err)
	}
}

func TestStorePersistProviderTerminalHandoffAcceptsExpiredNowWhenObservedBeforeExpiry(t *testing.T) {
	t.Parallel()
	s, db, settlementReq := phase2ProviderSettlementFixture(t)
	s.now = func() time.Time { return time.Date(2026, 3, 1, 12, 20, 0, 0, time.UTC) }
	req := terminalHandoffRequest(settlementReq)

	result, err := s.PersistProviderTerminalHandoff(context.Background(), req)
	if err != nil {
		t.Fatalf("persist handoff observed before expired lease: %v", err)
	}
	if result.Idempotent || result.Handoff.CreatedAt != req.CreatedAt {
		t.Fatalf("unexpected expired-now handoff result: %#v", result)
	}
	if count := countPhase2Table(t, db, "provider_terminal_handoffs"); count != 1 {
		t.Fatalf("expired-now handoff rows = %d, want 1", count)
	}
}

func TestStorePersistProviderTerminalHandoffIsExactlyIdempotent(t *testing.T) {
	t.Parallel()
	s, db, settlementReq := phase2ProviderSettlementFixture(t)
	req := terminalHandoffRequest(settlementReq)
	req.PayloadJSON = `{ "terminal": true, "sequence": 1 }`

	first, err := s.PersistProviderTerminalHandoff(context.Background(), req)
	if err != nil {
		t.Fatalf("persist provider terminal handoff: %v", err)
	}
	if first.Idempotent || first.Handoff.HandoffID != ProviderTerminalHandoffID(req.ProviderInvocationID) || first.Handoff.PayloadJSON != req.PayloadJSON {
		t.Fatalf("unexpected first handoff: %#v", first)
	}
	replayReq := req
	replay, err := s.PersistProviderTerminalHandoff(context.Background(), replayReq)
	if err != nil || !replay.Idempotent || replay.Handoff != first.Handoff {
		t.Fatalf("exact handoff replay did not converge: first=%#v replay=%#v err=%v", first, replay, err)
	}

	changedPayload := req
	changedPayload.PayloadJSON = `{"terminal":false}`
	if _, err := s.PersistProviderTerminalHandoff(context.Background(), changedPayload); err == nil {
		t.Fatal("expected changed handoff payload to conflict")
	} else {
		requireProviderSettlementError(t, err, "persist_provider_terminal_handoff", "terminal_handoff_conflict")
	}
	changedFormatting := req
	changedFormatting.PayloadJSON = `{"sequence":1,"terminal":true}`
	if _, err := s.PersistProviderTerminalHandoff(context.Background(), changedFormatting); err == nil {
		t.Fatal("expected reformatted handoff payload to conflict")
	} else {
		requireProviderSettlementError(t, err, "persist_provider_terminal_handoff", "terminal_handoff_conflict")
	}
	changedObservation := req
	changedObservation.StreamObservation.Tokens.TotalTokens++
	if _, err := s.PersistProviderTerminalHandoff(context.Background(), changedObservation); err == nil {
		t.Fatal("expected changed handoff observation to conflict")
	} else {
		requireProviderSettlementError(t, err, "persist_provider_terminal_handoff", "terminal_handoff_conflict")
	}
	changedLeaseLineage := req
	changedLeaseLineage.ExpectedLeaseVersion++
	if _, err := s.PersistProviderTerminalHandoff(context.Background(), changedLeaseLineage); err == nil || !strings.Contains(err.Error(), "exact bound observation and lease") {
		t.Fatalf("expected changed handoff lease lineage to be rejected, got %v", err)
	}
	changedCreatedAt := req
	changedCreatedAt.CreatedAt = "2026-03-01T12:00:03Z"
	if _, err := s.PersistProviderTerminalHandoff(context.Background(), changedCreatedAt); err == nil {
		t.Fatal("expected changed handoff created_at to conflict")
	} else {
		requireProviderSettlementError(t, err, "persist_provider_terminal_handoff", "terminal_handoff_conflict")
	}
	oversized := req
	oversized.ProviderInvocationID = "another-invocation"
	oversized.PayloadJSON = `"` + strings.Repeat("x", maxProviderTerminalHandoffPayloadBytes) + `"`
	if _, err := s.PersistProviderTerminalHandoff(context.Background(), oversized); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected oversized handoff payload rejection, got %v", err)
	}
	if count := countPhase2Table(t, db, "provider_terminal_handoffs"); count != 1 {
		t.Fatalf("expected one canonical handoff row, got %d", count)
	}
}

func TestStoreListPendingProviderTerminalHandoffsUsesInvocationKeyset(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	s.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 4, 0, time.UTC) }
	ctx := context.Background()

	for i, invocationID := range []string{"handoff_invocation_a", "handoff_invocation_b", "handoff_invocation_c"} {
		sequence := i + 2
		nodeRunID := fmt.Sprintf("nr_handoff_%d", sequence)
		attemptID := fmt.Sprintf("run_1:handoff:%d", sequence)
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version) VALUES ('%s', 'run_1', 'work_intake', %d, %d, 'running', 'governance', 'agent_governance', 1)`, nodeRunID, sequence, sequence))
		if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: nodeRunID, ExecutionMode: ExecutionModeProviderBacked, CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
			t.Fatalf("enqueue handoff fixture %q: %v", invocationID, err)
		}
		claim, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "worker-" + invocationID, LeaseUntil: "2026-03-01T12:10:00Z", ClaimedAt: "2026-03-01T12:00:01Z"})
		if err != nil || !acquired {
			t.Fatalf("claim handoff fixture %q: claim=%#v acquired=%t err=%v", invocationID, claim, acquired, err)
		}
		if _, err := db.Exec(`UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:00:02Z' WHERE attempt_id = ?`, attemptID); err != nil {
			t.Fatalf("mark handoff fixture %q running: %v", invocationID, err)
		}
		sessionID := "session-" + invocationID
		if _, err := activateProviderInvocationForTest(t, s, ctx, ActivateProviderInvocationRequest{ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: nodeRunID, LogicalSessionID: "logical-" + invocationID, ProviderSessionID: sessionID, CompletionMode: ProviderCompletionModeServerResponse, IssuedAt: "2026-03-01T12:00:02Z", CreatedAt: "2026-03-01T12:00:02Z", OpenedAt: "2026-03-01T12:00:02Z"}); err != nil {
			t.Fatalf("activate handoff fixture %q: %v", invocationID, err)
		}
		if _, err := s.PersistProviderTerminalHandoff(ctx, PersistProviderTerminalHandoffRequest{ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: nodeRunID, ExpectedLeaseOwner: "worker-" + invocationID, ExpectedLeaseVersion: claim.LeaseVersion, StreamObservation: boundSuccessObservation(invocationID, sessionID, "response"), PayloadJSON: fmt.Sprintf(`{"sequence":%d}`, sequence), CreatedAt: "2026-03-01T12:00:03Z"}); err != nil {
			t.Fatalf("persist handoff fixture %q: %v", invocationID, err)
		}
		if invocationID == "handoff_invocation_c" {
			if _, err := db.Exec(`UPDATE attempt_executions SET state = 'committed_failure', terminal_observed_at = '2026-03-01T12:00:04Z', committed_at = '2026-03-01T12:00:04Z' WHERE attempt_id = ?`, attemptID); err != nil {
				t.Fatalf("terminalize filtered handoff fixture: %v", err)
			}
		}
	}

	first, err := s.ListPendingProviderTerminalHandoffs(ctx, "", 1)
	if err != nil || len(first) != 1 || first[0].ProviderInvocationID != "handoff_invocation_a" {
		t.Fatalf("unexpected first handoff page: handoffs=%#v err=%v", first, err)
	}
	second, err := s.ListPendingProviderTerminalHandoffs(ctx, first[0].ProviderInvocationID, 1)
	if err != nil || len(second) != 1 || second[0].ProviderInvocationID != "handoff_invocation_b" {
		t.Fatalf("unexpected second handoff page: handoffs=%#v err=%v", second, err)
	}
	last, err := s.ListPendingProviderTerminalHandoffs(ctx, second[0].ProviderInvocationID, 1)
	if err != nil || len(last) != 0 {
		t.Fatalf("terminal attempt handoff leaked into pending page: handoffs=%#v err=%v", last, err)
	}
	if _, err := s.ListPendingProviderTerminalHandoffs(ctx, "", 0); err == nil {
		t.Fatal("expected a required positive handoff page limit")
	}
}

func TestStoreSettleProviderCompletionConsumesTerminalHandoffAfterLeaseExpiry(t *testing.T) {
	t.Parallel()
	s, db, req := phase2ProviderSettlementFixture(t)
	handoff, err := s.PersistProviderTerminalHandoff(context.Background(), terminalHandoffRequest(req))
	if err != nil {
		t.Fatalf("persist terminal handoff: %v", err)
	}
	req.TerminalHandoffID = handoff.Handoff.HandoffID
	if _, err := db.Exec(`UPDATE attempt_executions SET lease_expires_at = '2026-03-01T12:00:03Z' WHERE attempt_id = ?`, req.Settlement.AttemptID); err != nil {
		t.Fatalf("expire settlement lease: %v", err)
	}

	settlement, err := s.SettleProviderCompletionAndTerminal(context.Background(), req)
	if err != nil {
		t.Fatalf("settle from durable terminal handoff: %v", err)
	}
	if _, found, err := s.GetProviderTerminalHandoff(context.Background(), handoff.Handoff.HandoffID); err != nil || found {
		t.Fatalf("settled terminal handoff was not deleted atomically: found=%t err=%v", found, err)
	}
	if replay, err := s.SettleProviderCompletionAndTerminal(context.Background(), req); err != nil || replay != settlement {
		t.Fatalf("already-settled replay required deleted handoff: settlement=%#v err=%v", replay, err)
	}
}

func TestStoreTerminalHandoffFencesSettlementWithoutItsClaim(t *testing.T) {
	t.Parallel()
	s, _, req := phase2ProviderSettlementFixture(t)
	handoff, err := s.PersistProviderTerminalHandoff(context.Background(), terminalHandoffRequest(req))
	if err != nil {
		t.Fatalf("persist terminal handoff: %v", err)
	}
	if _, err := s.SettleProviderCompletionAndTerminal(context.Background(), req); err == nil {
		t.Fatal("expected durable terminal handoff to fence settlement without its claim")
	} else {
		requireProviderSettlementError(t, err, "settle_provider_completion_and_terminal", "terminal_handoff_required")
	}
	if _, found, err := s.GetProviderTerminalHandoff(context.Background(), handoff.Handoff.HandoffID); err != nil || !found {
		t.Fatalf("fenced settlement removed terminal handoff: found=%t err=%v", found, err)
	}
}

func TestStoreNormalProviderSettlementRequiresTerminalHandoff(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		outcome ProviderCompletionOutcome
	}{
		{name: "success", outcome: ProviderCompletionOutcomeSuccess},
		{name: "failure", outcome: ProviderCompletionOutcomeFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, _, req := phase2ProviderSettlementFixture(t)
			if tc.outcome == ProviderCompletionOutcomeFailed {
				req.Settlement.Outcome = tc.outcome
				req.Settlement.FailureReasonCode = "provider_failed"
				req.Settlement.FailureReasonText = "provider execution failed"
				req.Terminal.TerminalState = "committed_failure"
				req.Terminal.TerminalReasonCode = "provider_failed"
				req.Terminal.FailureCode = "provider_failed"
				req.Terminal.FailureText = "provider execution failed"
			}
			_, err := s.SettleProviderCompletionAndTerminal(context.Background(), req)
			requireProviderSettlementError(t, err, "settle_provider_completion_and_terminal", "terminal_handoff_required")
		})
	}
}

func TestStoreCommittedSettlementFencesLaterTerminalHandoff(t *testing.T) {
	t.Parallel()
	s, _, req := phase2ProviderSettlementFixture(t)
	attachTerminalHandoffToSettlement(t, s, &req)
	if _, err := s.SettleProviderCompletionAndTerminal(context.Background(), req); err != nil {
		t.Fatalf("commit provider settlement: %v", err)
	}
	_, err := s.PersistProviderTerminalHandoff(context.Background(), terminalHandoffRequest(req))
	requireProviderSettlementError(t, err, "persist_provider_terminal_handoff", "invocation_not_active")
}

func TestStoreProviderTerminalHandoffAcceptsPayloadLargerThanSixtyFourKiB(t *testing.T) {
	t.Parallel()
	s, _, req := phase2ProviderSettlementFixture(t)
	handoffReq := terminalHandoffRequest(req)
	handoffReq.PayloadJSON = `{"diagnostics":"` + strings.Repeat("x", 128*1024) + `"}`
	result, err := s.PersistProviderTerminalHandoff(context.Background(), handoffReq)
	if err != nil {
		t.Fatalf("persist realistic large terminal handoff: %v", err)
	}
	if len(result.Handoff.PayloadJSON) <= 64*1024 {
		t.Fatalf("terminal handoff payload was unexpectedly small: %d", len(result.Handoff.PayloadJSON))
	}
}

func TestStoreSettleProviderCompletionRejectsChangedTerminalHandoffObservation(t *testing.T) {
	t.Parallel()
	s, db, req := phase2ProviderSettlementFixture(t)
	handoff, err := s.PersistProviderTerminalHandoff(context.Background(), terminalHandoffRequest(req))
	if err != nil {
		t.Fatalf("persist terminal handoff: %v", err)
	}
	req.TerminalHandoffID = handoff.Handoff.HandoffID
	req.StreamObservation.Tokens.TotalTokens++
	if _, err := db.Exec(`UPDATE attempt_executions SET lease_expires_at = '2026-03-01T12:00:03Z' WHERE attempt_id = ?`, req.Settlement.AttemptID); err != nil {
		t.Fatalf("expire settlement lease: %v", err)
	}

	_, err = s.SettleProviderCompletionAndTerminal(context.Background(), req)
	requireProviderSettlementError(t, err, "settle_provider_completion_and_terminal", "terminal_handoff_conflict")
	if _, found, err := s.GetProviderTerminalHandoff(context.Background(), handoff.Handoff.HandoffID); err != nil || !found {
		t.Fatalf("mismatched settlement removed terminal handoff: found=%t err=%v", found, err)
	}
}

func TestStoreSettleProviderCompletionFailureRetainsTerminalHandoff(t *testing.T) {
	t.Parallel()
	s, db, req := phase2ProviderSettlementFixture(t)
	handoff, err := s.PersistProviderTerminalHandoff(context.Background(), terminalHandoffRequest(req))
	if err != nil {
		t.Fatalf("persist terminal handoff: %v", err)
	}
	req.TerminalHandoffID = handoff.Handoff.HandoffID
	if _, err := db.Exec(`UPDATE attempt_executions SET lease_expires_at = '2026-03-01T12:00:03Z' WHERE attempt_id = ?`, req.Settlement.AttemptID); err != nil {
		t.Fatalf("expire settlement lease: %v", err)
	}
	mustExec(t, db, `CREATE TRIGGER injected_handoff_delete_failure BEFORE DELETE ON provider_terminal_handoffs BEGIN SELECT RAISE(ABORT, 'injected terminal handoff delete failure'); END`)

	if _, err := s.SettleProviderCompletionAndTerminal(context.Background(), req); err == nil || !strings.Contains(err.Error(), "injected terminal handoff delete failure") {
		t.Fatalf("expected injected settlement failure, got %v", err)
	}
	if _, found, err := s.GetProviderTerminalHandoff(context.Background(), handoff.Handoff.HandoffID); err != nil || !found {
		t.Fatalf("failed settlement did not retain terminal handoff: found=%t err=%v", found, err)
	}
	for _, table := range []string{"provider_completion_settlements", "attempt_event_log", "attempt_outbox", "token_usage"} {
		if count := countPhase2Table(t, db, table); count != 0 {
			t.Fatalf("failed handoff settlement left %d rows in %s", count, table)
		}
	}
}

func TestStoreSettleProviderCompletionAndTerminalRollsBackEveryWriteStage(t *testing.T) {
	tests := []struct {
		name    string
		trigger string
	}{
		{
			name: "primary stream evidence",
			trigger: `CREATE TRIGGER phase2_injected_failure BEFORE INSERT ON provider_completion_evidence
				WHEN NEW.evidence_kind = 'stream_observation'
				BEGIN SELECT RAISE(ABORT, 'injected failure: primary stream evidence'); END`,
		},
		{
			name: "settlement insert",
			trigger: `CREATE TRIGGER phase2_injected_failure BEFORE INSERT ON provider_completion_settlements
				BEGIN SELECT RAISE(ABORT, 'injected failure: settlement insert'); END`,
		},
		{
			name: "invocation terminal update",
			trigger: `CREATE TRIGGER phase2_injected_failure BEFORE UPDATE OF invocation_state ON provider_invocations
				WHEN NEW.invocation_state = 'settled_success'
				BEGIN SELECT RAISE(ABORT, 'injected failure: invocation terminal update'); END`,
		},
		{
			name: "attempt canonical settlement link",
			trigger: `CREATE TRIGGER phase2_injected_failure BEFORE UPDATE OF canonical_settlement_id ON attempt_executions
				WHEN COALESCE(OLD.canonical_settlement_id, '') = '' AND COALESCE(NEW.canonical_settlement_id, '') <> ''
				BEGIN SELECT RAISE(ABORT, 'injected failure: attempt canonical settlement link'); END`,
		},
		{
			name: "attempt terminal update and lease clear",
			trigger: `CREATE TRIGGER phase2_injected_failure BEFORE UPDATE OF state, lease_owner, lease_expires_at ON attempt_executions
				WHEN NEW.state = 'committed_success' AND NEW.lease_owner = '' AND NEW.lease_expires_at IS NULL
				BEGIN SELECT RAISE(ABORT, 'injected failure: attempt terminal update and lease clear'); END`,
		},
		{
			name: "invocation token usage",
			trigger: `CREATE TRIGGER phase2_injected_failure BEFORE INSERT ON token_usage
				WHEN NEW.provider_invocation_id IS NOT NULL
				BEGIN SELECT RAISE(ABORT, 'injected failure: invocation token usage'); END`,
		},
		{
			name: "terminal attempt event",
			trigger: `CREATE TRIGGER phase2_injected_failure BEFORE INSERT ON attempt_event_log
				WHEN NEW.event_type = 'attempt.succeeded'
				BEGIN SELECT RAISE(ABORT, 'injected failure: terminal attempt event'); END`,
		},
		{
			name: "outbox",
			trigger: `CREATE TRIGGER phase2_injected_failure BEFORE INSERT ON attempt_outbox
				BEGIN SELECT RAISE(ABORT, 'injected failure: outbox'); END`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, db, req := phase2ProviderSettlementFixture(t)
			attachTerminalHandoffToSettlement(t, s, &req)
			before := capturePhase2ProviderSettlementState(t, s, db, req.Settlement.AttemptID, req.Settlement.ProviderInvocationID)
			if before.Attempt.LeaseOwner != req.ExpectedLeaseOwner || before.Attempt.LeaseExpiresAt == "" || before.Attempt.LeaseVersion != req.ExpectedLeaseVersion {
				t.Fatalf("fixture is missing original lease lineage: %#v", before.Attempt)
			}
			if before.Invocation.InvocationState != ProviderInvocationStateOpen {
				t.Fatalf("fixture invocation was not open before settlement: %#v", before.Invocation)
			}
			if _, err := db.Exec(tc.trigger); err != nil {
				t.Fatalf("install failure trigger: %v", err)
			}

			_, err := s.SettleProviderCompletionAndTerminal(context.Background(), req)
			if err == nil || !strings.Contains(err.Error(), "injected failure: "+tc.name) {
				t.Fatalf("expected injected %s failure, got %v", tc.name, err)
			}
			after := capturePhase2ProviderSettlementState(t, s, db, req.Settlement.AttemptID, req.Settlement.ProviderInvocationID)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("%s failure did not roll back the complete transaction\nbefore: %#v\nafter:  %#v", tc.name, before, after)
			}
		})
	}
}

func TestStoreSettleProviderCompletionAndTerminalFailureRacesSuccess(t *testing.T) {
	s, db, successReq := phase2ProviderSettlementFixture(t)
	attachTerminalHandoffToSettlement(t, s, &successReq)
	db.SetMaxOpenConns(1)

	failureReq := successReq
	failureReq.Settlement = FinalizeProviderCompletionSettlementRequest{
		SettlementID:            "settlement_phase2_failed",
		ProviderInvocationID:    successReq.Settlement.ProviderInvocationID,
		AttemptID:               successReq.Settlement.AttemptID,
		RunID:                   successReq.Settlement.RunID,
		NodeRunID:               successReq.Settlement.NodeRunID,
		Outcome:                 ProviderCompletionOutcomeFailed,
		ProviderDiagnosticsJSON: `{}`,
		FailureReasonCode:       "llm_invalid_response",
		FailureReasonText:       "invalid response won terminal authority",
		SettledAt:               successReq.Settlement.SettledAt,
	}
	failureReq.Terminal = CommitTerminalRequest{
		OutboxID:           "outbox_phase2_failed",
		AttemptID:          successReq.Terminal.AttemptID,
		RunID:              successReq.Terminal.RunID,
		NodeRunID:          successReq.Terminal.NodeRunID,
		TerminalState:      "committed_failure",
		PayloadJSON:        `{"failed":true}`,
		CommittedAt:        successReq.Terminal.CommittedAt,
		TerminalReasonCode: "llm_invalid_response",
		FailureCode:        "llm_invalid_response",
		FailureText:        "invalid response won terminal authority",
		ProviderSessionID:  successReq.Terminal.ProviderSessionID,
	}

	type callResult struct {
		name       string
		settlement ProviderCompletionSettlement
		err        error
	}
	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	results := make(chan callResult, 2)
	launch := func(name string, req SettleProviderCompletionAndTerminalRequest) {
		go func() {
			ready <- struct{}{}
			<-start
			settlement, err := s.SettleProviderCompletionAndTerminal(context.Background(), req)
			results <- callResult{name: name, settlement: settlement, err: err}
		}()
	}
	launch("success", successReq)
	launch("failure", failureReq)
	<-ready
	<-ready
	close(start)

	completed := map[string]callResult{}
	for range 2 {
		result := <-results
		completed[result.name] = result
	}
	successResult := completed["success"]
	failureResult := completed["failure"]
	if (successResult.err == nil) == (failureResult.err == nil) {
		t.Fatalf("expected exactly one terminal authority winner, success=%v failure=%v", successResult.err, failureResult.err)
	}

	winnerReq, loserReq := successReq, failureReq
	winnerResult, loserResult := successResult, failureResult
	if failureResult.err == nil {
		winnerReq, loserReq = failureReq, successReq
		winnerResult, loserResult = failureResult, successResult
	}
	requireLostTerminalRace(t, loserResult.err)
	if replayed, err := s.SettleProviderCompletionAndTerminal(context.Background(), winnerReq); err != nil || replayed != winnerResult.settlement {
		t.Fatalf("winner exact replay did not converge: settlement=%#v err=%v", replayed, err)
	}
	_, err := s.SettleProviderCompletionAndTerminal(context.Background(), loserReq)
	requireLostTerminalRace(t, err)

	assertPhase2TerminalWinner(t, s, db, winnerReq, winnerResult.settlement)
	loserSettlementCount := countPhase2Rows(t, db, "provider_completion_settlements", "settlement_id", loserReq.Settlement.SettlementID)
	loserOutboxCount := countPhase2Rows(t, db, "attempt_outbox", "outbox_id", loserReq.Terminal.OutboxID)
	if loserSettlementCount != 0 || loserOutboxCount != 0 {
		t.Fatalf("losing terminal outcome left partial rows: settlements=%d outbox=%d", loserSettlementCount, loserOutboxCount)
	}
}

// requireLostTerminalRace asserts that a settlement lost to the request's one
// canonical outcome, which is fixed by the first settlement to commit.
func requireLostTerminalRace(t *testing.T, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "canonical settled outcome") {
		t.Fatalf("expected the losing outcome to conflict with the canonical settlement, got %v", err)
	}
}

func atomicProviderSettlementFixture(t *testing.T) (*Store, *sql.DB, SettleProviderCompletionAndTerminalRequest) {
	t.Helper()
	db := openTestDB(t)
	return atomicProviderSettlementFixtureWithDB(t, db)
}

func atomicProviderSettlementFixtureWithDB(t *testing.T, db *sql.DB) (*Store, *sql.DB, SettleProviderCompletionAndTerminalRequest) {
	t.Helper()
	s, db, req := unfinishedAtomicProviderSettlementFixtureWithDB(t, db)
	finishBoundRequest(t, s, &req)
	return s, db, req
}

// unfinishedAtomicProviderSettlementFixtureWithDB activates the request but
// leaves it open, so a test can admit commands before the server finishes it.
func unfinishedAtomicProviderSettlementFixtureWithDB(t *testing.T, db *sql.DB) (*Store, *sql.DB, SettleProviderCompletionAndTerminalRequest) {
	t.Helper()
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	s.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 4, 0, time.UTC) }
	ctx := context.Background()
	const attemptID = "run_1:atomic_lease_rejection:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: ExecutionModeProviderBacked, CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatalf("enqueue fixture attempt: %v", err)
	}
	claim, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "worker-a", LeaseUntil: "2026-03-01T12:10:00Z", ClaimedAt: "2026-03-01T12:00:01Z"})
	if err != nil || !acquired || claim.LeaseVersion != 1 {
		t.Fatalf("claim fixture attempt: claim=%#v acquired=%t err=%v", claim, acquired, err)
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:00:02Z' WHERE attempt_id = ?`, attemptID); err != nil {
		t.Fatalf("mark fixture attempt running: %v", err)
	}
	if _, err := activateProviderInvocationForTest(t, s, ctx, ActivateProviderInvocationRequest{ProviderInvocationID: "invocation_atomic_lease_rejection", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical_atomic", CompletionMode: ProviderCompletionModeServerResponse, ProviderSessionID: "ses_atomic", IssuedAt: "2026-03-01T12:00:02Z", CreatedAt: "2026-03-01T12:00:02Z", OpenedAt: "2026-03-01T12:00:02Z"}); err != nil {
		t.Fatalf("activate fixture invocation: %v", err)
	}
	observation := boundSuccessObservation("invocation_atomic_lease_rejection", "ses_atomic", "canonical response")
	return s, db, SettleProviderCompletionAndTerminalRequest{
		Settlement:           FinalizeProviderCompletionSettlementRequest{SettlementID: "settlement_atomic_lease_rejection", ProviderInvocationID: "invocation_atomic_lease_rejection", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", Outcome: ProviderCompletionOutcomeSuccess, FinalText: "canonical response", StructuredPayloadJSON: `{}`, ProviderDiagnosticsJSON: `{}`, SettledAt: "2026-03-01T12:00:04Z"},
		StreamObservation:    &observation,
		ExpectedLeaseOwner:   "worker-a",
		ExpectedLeaseVersion: 1,
		TokenUsage:           &ProviderInvocationTokenUsage{RequiredRole: "governance", CreatedAt: "2026-03-01T12:00:04Z"},
		Terminal:             CommitTerminalRequest{OutboxID: "outbox_atomic_lease_rejection", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", TerminalState: "committed_success", PayloadJSON: `{"output_text":"canonical response"}`, CommittedAt: "2026-03-01T12:00:04Z", ProviderSessionID: "ses_atomic"},
	}
}

type phase2ProviderSettlementState struct {
	Attempt         AttemptExecution
	Invocation      ProviderInvocation
	Evidence        []ProviderCompletionEvidence
	SettlementCount int
	EventCount      int
	OutboxCount     int
	TokenUsageCount int
}

func phase2ProviderSettlementFixture(t *testing.T) (*Store, *sql.DB, SettleProviderCompletionAndTerminalRequest) {
	t.Helper()
	return phase2ProviderSettlementFixtureWithDB(t, openTestDB(t))
}

func phase2ProviderSettlementFixtureWithDB(t *testing.T, db *sql.DB) (*Store, *sql.DB, SettleProviderCompletionAndTerminalRequest) {
	t.Helper()
	s, db, req := unfinishedPhase2ProviderSettlementFixtureWithDB(t, db)
	finishBoundRequest(t, s, &req)
	return s, db, req
}

// unfinishedPhase2ProviderSettlementFixture activates the request but leaves it
// open, so a test can shape what the server finishes it with.
func unfinishedPhase2ProviderSettlementFixture(t *testing.T) (*Store, *sql.DB, SettleProviderCompletionAndTerminalRequest) {
	t.Helper()
	return unfinishedPhase2ProviderSettlementFixtureWithDB(t, openTestDB(t))
}

func unfinishedPhase2ProviderSettlementFixtureWithDB(t *testing.T, db *sql.DB) (*Store, *sql.DB, SettleProviderCompletionAndTerminalRequest) {
	t.Helper()
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	s.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 4, 0, time.UTC) }
	ctx := context.Background()
	const attemptID = "run_1:phase2_settlement:1"
	const invocationID = "invocation_phase2_settlement"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: ExecutionModeProviderBacked, CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatalf("enqueue phase 2 fixture attempt: %v", err)
	}
	claim, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "worker-phase2", LeaseUntil: "2026-03-01T12:10:00Z", ClaimedAt: "2026-03-01T12:00:01Z"})
	if err != nil || !acquired || claim.LeaseVersion != 1 {
		t.Fatalf("claim phase 2 fixture attempt: claim=%#v acquired=%t err=%v", claim, acquired, err)
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:00:02Z' WHERE attempt_id = ?`, attemptID); err != nil {
		t.Fatalf("mark phase 2 fixture attempt running: %v", err)
	}
	if _, err := activateProviderInvocationForTest(t, s, ctx, ActivateProviderInvocationRequest{ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical_phase2", CompletionMode: ProviderCompletionModeServerResponse, ProviderSessionID: "ses_phase2", IssuedAt: "2026-03-01T12:00:02Z", CreatedAt: "2026-03-01T12:00:02Z", OpenedAt: "2026-03-01T12:00:02Z"}); err != nil {
		t.Fatalf("activate phase 2 fixture invocation: %v", err)
	}
	observation := boundSuccessObservation(invocationID, "ses_phase2", "canonical response")
	observation.Tokens = providerexecution.TokenUsage{InputTokens: 10, OutputTokens: 4, TotalTokens: 14, ReasoningTokens: 2}
	return s, db, SettleProviderCompletionAndTerminalRequest{
		Settlement:           FinalizeProviderCompletionSettlementRequest{SettlementID: "settlement_phase2_success", ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", Outcome: ProviderCompletionOutcomeSuccess, FinalText: "canonical response", StructuredPayloadJSON: `{}`, ProviderDiagnosticsJSON: `{}`, SettledAt: "2026-03-01T12:00:04Z"},
		StreamObservation:    &observation,
		ExpectedLeaseOwner:   "worker-phase2",
		ExpectedLeaseVersion: 1,
		TokenUsage:           &ProviderInvocationTokenUsage{RequiredRole: "governance", Model: "github-copilot/gpt-5.4-mini", Variant: "low", CreatedAt: "2026-03-01T12:00:04Z"},
		Terminal:             CommitTerminalRequest{OutboxID: "outbox_phase2_success", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", TerminalState: "committed_success", PayloadJSON: `{"output_text":"canonical response"}`, CommittedAt: "2026-03-01T12:00:04Z", ProviderSessionID: "ses_phase2"},
	}
}

func terminalHandoffRequest(req SettleProviderCompletionAndTerminalRequest) PersistProviderTerminalHandoffRequest {
	return PersistProviderTerminalHandoffRequest{
		ProviderInvocationID: req.Settlement.ProviderInvocationID,
		AttemptID:            req.Settlement.AttemptID,
		RunID:                req.Settlement.RunID,
		NodeRunID:            req.Settlement.NodeRunID,
		ExpectedLeaseOwner:   req.ExpectedLeaseOwner,
		ExpectedLeaseVersion: req.ExpectedLeaseVersion,
		StreamObservation:    *req.StreamObservation,
		PayloadJSON:          `{"terminal_state":"committed_success"}`,
		CreatedAt:            "2026-03-01T12:00:04Z",
	}
}

func attachTerminalHandoffToSettlement(t *testing.T, store *Store, req *SettleProviderCompletionAndTerminalRequest) {
	t.Helper()
	result, err := store.PersistProviderTerminalHandoff(context.Background(), terminalHandoffRequest(*req))
	if err != nil {
		t.Fatalf("persist required terminal handoff: %v", err)
	}
	req.TerminalHandoffID = result.Handoff.HandoffID
}

func acknowledgeProviderProcessGuard(t *testing.T, s *Store, invocationID, attemptID, runID, nodeRunID, leaseOwner string, leaseVersion int64) {
	t.Helper()
	claimBoundCommands(t, s, invocationID, 1)
	if _, err := s.AcknowledgeProviderProcessGuard(context.Background(), AcknowledgeProviderProcessGuardRequest{
		GuardID:               "guard_" + sanitizeProviderSettlementToken(invocationID),
		ProviderInvocationID:  invocationID,
		AttemptID:             attemptID,
		RunID:                 runID,
		NodeRunID:             nodeRunID,
		CommandOrdinal:        1,
		Platform:              "unix_process_group",
		GuardianPID:           1001,
		GuardianIdentityToken: "guardian-start-1001",
		ChildPID:              1002,
		ChildIdentityToken:    "child-start-1002",
		TreeID:                "1002",
		TreeIdentityToken:     "tree-start-1002",
		QuiescenceCapability:  "quiescence-capability-" + invocationID,
		ExpectedLeaseOwner:    leaseOwner,
		ExpectedLeaseVersion:  leaseVersion,
		StartedAt:             "2026-03-01T12:00:02Z",
		AcknowledgedAt:        "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("acknowledge provider process guard: %v", err)
	}
}

func acknowledgeQuiescentProviderProcessGuard(t *testing.T, s *Store, invocationID, attemptID, runID, nodeRunID, leaseOwner string, leaseVersion int64) {
	t.Helper()
	acknowledgeProviderProcessGuard(t, s, invocationID, attemptID, runID, nodeRunID, leaseOwner, leaseVersion)
	guardID := "guard_" + sanitizeProviderSettlementToken(invocationID)
	evidence := `{"type":"result","result":{"GuardID":"` + guardID + `","ChildPID":1002,"TreeID":1002,"Platform":"unix_process_group","Quiescent":true,"ExitCode":0,"ExitSignal":0,"Forced":false,"FailureCode":"","FailureText":""}}`
	if _, err := s.IngestProviderProcessGuardQuiescence(context.Background(), IngestProviderProcessGuardQuiescenceRequest{
		GuardID:               guardID,
		ProviderInvocationID:  invocationID,
		AttemptID:             attemptID,
		RunID:                 runID,
		NodeRunID:             nodeRunID,
		Platform:              "unix_process_group",
		GuardianPID:           1001,
		GuardianIdentityToken: "guardian-start-1001",
		ChildPID:              1002,
		ChildIdentityToken:    "child-start-1002",
		TreeID:                "1002",
		TreeIdentityToken:     "tree-start-1002",
		QuiescenceCapability:  "quiescence-capability-" + invocationID,
		ExitedAt:              "2026-03-01T12:00:03Z",
		QuiescentAt:           "2026-03-01T12:00:03Z",
		EvidenceSHA256:        sha256Hex(evidence),
		EvidenceJSON:          evidence,
	}); err != nil {
		t.Fatalf("ingest provider process guard quiescence: %v", err)
	}
	completeBoundCommand(t, s, invocationID, guardID, 1)
}

func capturePhase2ProviderSettlementState(t *testing.T, s *Store, db *sql.DB, attemptID, invocationID string) phase2ProviderSettlementState {
	t.Helper()
	attempt, found, err := s.GetAttempt(context.Background(), attemptID)
	if err != nil || !found {
		t.Fatalf("load phase 2 attempt snapshot: found=%t err=%v", found, err)
	}
	invocation, found, err := s.GetProviderInvocation(context.Background(), invocationID)
	if err != nil || !found {
		t.Fatalf("load phase 2 invocation snapshot: found=%t err=%v", found, err)
	}
	evidence, err := s.ListProviderCompletionEvidence(context.Background(), invocationID)
	if err != nil {
		t.Fatalf("load phase 2 evidence snapshot: %v", err)
	}
	return phase2ProviderSettlementState{
		Attempt:         attempt,
		Invocation:      invocation,
		Evidence:        evidence,
		SettlementCount: countPhase2Table(t, db, "provider_completion_settlements"),
		EventCount:      countPhase2Table(t, db, "attempt_event_log"),
		OutboxCount:     countPhase2Table(t, db, "attempt_outbox"),
		TokenUsageCount: countPhase2Table(t, db, "token_usage"),
	}
}

func assertPhase2TerminalWinner(t *testing.T, s *Store, db *sql.DB, req SettleProviderCompletionAndTerminalRequest, settlement ProviderCompletionSettlement) {
	t.Helper()
	attempt, found, err := s.GetAttempt(context.Background(), req.Settlement.AttemptID)
	if err != nil || !found {
		t.Fatalf("load terminal winner attempt: found=%t err=%v", found, err)
	}
	if attempt.State != req.Terminal.TerminalState || attempt.CanonicalSettlementID != settlement.SettlementID || attempt.ActiveProviderInvocationID != req.Settlement.ProviderInvocationID || attempt.LeaseOwner != "" || attempt.LeaseExpiresAt != "" || attempt.LeaseVersion != req.ExpectedLeaseVersion {
		t.Fatalf("terminal winner left mixed attempt truth: %#v", attempt)
	}
	invocation, found, err := s.GetProviderInvocation(context.Background(), req.Settlement.ProviderInvocationID)
	if err != nil || !found {
		t.Fatalf("load terminal winner invocation: found=%t err=%v", found, err)
	}
	expectedInvocationState := ProviderInvocationStateSettledSuccess
	switch settlement.Outcome {
	case ProviderCompletionOutcomeFailed:
		expectedInvocationState = ProviderInvocationStateSettledFailed
	case ProviderCompletionOutcomeCancelled:
		expectedInvocationState = ProviderInvocationStateSettledCancelled
	}
	if invocation.InvocationState != expectedInvocationState || invocation.SupersededByInvocationID != "" {
		t.Fatalf("terminal winner left mixed invocation truth: %#v", invocation)
	}
	evidence, err := s.ListProviderCompletionEvidence(context.Background(), invocation.ProviderInvocationID)
	if err != nil {
		t.Fatalf("load terminal winner evidence: %v", err)
	}
	if len(evidence) != 1 || evidence[0].EvidenceID != settlement.PrimaryEvidenceID || evidence[0].EvidenceKind != ProviderCompletionEvidenceKindStreamObservation {
		t.Fatalf("terminal settlement did not use the durable stream observation as primary evidence: settlement=%#v evidence=%#v", settlement, evidence)
	}
	for table, expected := range map[string]int{
		"provider_completion_evidence":    1,
		"provider_completion_settlements": 1,
		"attempt_event_log":               1,
		"attempt_outbox":                  1,
		"token_usage":                     1,
	} {
		if count := countPhase2Table(t, db, table); count != expected {
			t.Fatalf("expected %d canonical rows in %s, got %d", expected, table, count)
		}
	}
	if count := countPhase2Rows(t, db, "provider_completion_settlements", "settlement_id", settlement.SettlementID); count != 1 {
		t.Fatalf("expected winning settlement %q exactly once, got %d", settlement.SettlementID, count)
	}
	storedSettlement, found, err := s.GetProviderCompletionSettlement(context.Background(), settlement.SettlementID)
	if err != nil || !found {
		t.Fatalf("load winning settlement: found=%t err=%v", found, err)
	}
	if storedSettlement != settlement {
		t.Fatalf("stored settlement differs from race winner: stored=%#v winner=%#v", storedSettlement, settlement)
	}
	var eventType, eventReasonCode, eventPayload string
	if err := db.QueryRow(`SELECT event_type, reason_code, payload_json FROM attempt_event_log WHERE attempt_id = ?`, req.Settlement.AttemptID).Scan(&eventType, &eventReasonCode, &eventPayload); err != nil {
		t.Fatalf("load winning terminal event: %v", err)
	}
	expectedEventType, expectedReasonCode := terminalAttemptEventForState(req.Terminal.TerminalState, req.Terminal.TerminalReasonCode, req.Terminal.FailureCode)
	if eventType != expectedEventType || eventReasonCode != expectedReasonCode || eventPayload != strings.TrimSpace(req.Terminal.PayloadJSON) {
		t.Fatalf("terminal event differs from race winner: type=%q reason=%q payload=%q", eventType, eventReasonCode, eventPayload)
	}
	if count := countPhase2Rows(t, db, "attempt_outbox", "outbox_id", req.Terminal.OutboxID); count != 1 {
		t.Fatalf("expected winning outbox %q exactly once, got %d", req.Terminal.OutboxID, count)
	}
	var outboxState, outboxPayload string
	if err := db.QueryRow(`SELECT terminal_state, payload_json FROM attempt_outbox WHERE outbox_id = ?`, req.Terminal.OutboxID).Scan(&outboxState, &outboxPayload); err != nil {
		t.Fatalf("load winning outbox: %v", err)
	}
	if outboxState != req.Terminal.TerminalState || outboxPayload != strings.TrimSpace(req.Terminal.PayloadJSON) {
		t.Fatalf("terminal outbox differs from race winner: state=%q payload=%q", outboxState, outboxPayload)
	}
}

func requireProviderSettlementError(t *testing.T, err error, operation, reason string) {
	t.Helper()
	var settlementErr *ProviderSettlementError
	if !errors.As(err, &settlementErr) || settlementErr.Operation != operation || settlementErr.Reason != reason {
		t.Fatalf("expected %s/%s, got %v", operation, reason, err)
	}
}

func countPhase2Table(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM ` + table).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

func countPhase2Rows(t *testing.T, db *sql.DB, table, column, value string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM `+table+` WHERE `+column+` = ?`, value).Scan(&count); err != nil {
		t.Fatalf("count %s by %s: %v", table, column, err)
	}
	return count
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

	serverInvocation, err := activateProviderInvocationForTest(t, s, ctx, ActivateProviderInvocationRequest{
		ProviderInvocationID: "invocation_server_response",
		AttemptID:            serverAttemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_server_response",
		CompletionMode:       ProviderCompletionModeServerResponse,
		CreatedAt:            "2026-03-01T12:00:02Z",
	})
	if err != nil {
		t.Fatalf("activate server-response invocation: %v", err)
	}
	if serverInvocation.CompletionMode != ProviderCompletionModeServerResponse {
		t.Fatalf("expected server-response invocation, got %#v", serverInvocation)
	}

	tests := []struct {
		name string
		mode ProviderCompletionMode
	}{
		{name: "missing mode"},
		{name: "not applicable", mode: ProviderCompletionModeNotApplicable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := activateProviderInvocationForTest(t, s, ctx, ActivateProviderInvocationRequest{
				ProviderInvocationID: "invocation_" + strings.NewReplacer(" ", "_", "-", "_").Replace(tc.name),
				AttemptID:            serverAttemptID,
				RunID:                "run_1",
				NodeRunID:            "nr_1",
				CompletionMode:       tc.mode,
				CreatedAt:            "2026-03-01T12:00:03Z",
			})
			if err == nil {
				t.Fatal("expected invalid provider invocation completion contract to be rejected")
			}
		})
	}
}

func TestStoreActivateProviderInvocationRejectsRuntimeOwnedAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:runtime_owned_invocation:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeRuntimeOwned,
		CompletionMode: ProviderCompletionModeNotApplicable,
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue runtime-owned attempt: %v", err)
	}

	_, err := s.ActivateProviderInvocation(ctx, ActivateProviderInvocationRequest{
		ProviderInvocationID: "invocation_mismatched_mode",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		CompletionMode:       ProviderCompletionModeServerResponse,
		CreatedAt:            "2026-03-01T12:00:02Z",
		OpenedAt:             "2026-03-01T12:00:02Z",
		ExpectedLeaseOwner:   "worker-runtime-owned",
		ExpectedLeaseVersion: 1,
	})
	var settlementErr *ProviderSettlementError
	if !errors.As(err, &settlementErr) || settlementErr.Reason != "completion_mode_mismatch" {
		t.Fatalf("expected runtime-owned completion mode mismatch, got %v", err)
	}
	if _, found, err := s.GetProviderInvocation(ctx, "invocation_mismatched_mode"); err != nil {
		t.Fatalf("get mismatched invocation: %v", err)
	} else if found {
		t.Fatal("expected mismatched activation to roll back invocation creation")
	}
}

func TestStoreEnsureTerminalProviderCompletionEvidenceRejectsChangedReplay(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:work_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeServerResponse,
		SessionRef:     "session_run_1_work_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := activateProviderInvocationForTest(t, s, ctx, ActivateProviderInvocationRequest{
		ProviderInvocationID: "invocation_ensure_terminal_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_work_intake_1",
		ProviderSessionID:    "ses_ensure_terminal_1",
		CompletionMode:       ProviderCompletionModeServerResponse,
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("activate provider invocation: %v", err)
	}

	first, err := s.EnsureTerminalProviderCompletionEvidence(ctx, EnsureTerminalProviderCompletionEvidenceRequest{
		ProviderInvocationID: "invocation_ensure_terminal_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		State:                "failed",
		ReasonCode:           "provider_incomplete_turn",
		FailureText:          "provider terminal response was not clean",
		OutputText:           "Captured provider diagnostic output.",
		EvidenceSource:       "stream",
		CreatedAt:            "2026-03-01T12:00:04Z",
	})
	if err != nil {
		t.Fatalf("ensure first terminal evidence: %v", err)
	}

	_, err = s.EnsureTerminalProviderCompletionEvidence(ctx, EnsureTerminalProviderCompletionEvidenceRequest{
		ProviderInvocationID: "invocation_ensure_terminal_1",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		State:                "failed",
		ReasonCode:           "provider_incomplete_turn",
		FailureText:          "provider terminal response was not clean",
		OutputText:           "",
		EvidenceSource:       "stream",
		CreatedAt:            "2026-03-01T12:00:05Z",
	})
	var conflict *ProviderSettlementError
	if !errors.As(err, &conflict) || conflict.Reason != "evidence_conflict" {
		t.Fatalf("expected changed evidence replay conflict, got %v", err)
	}
	evidence, err := s.ListProviderCompletionEvidence(ctx, "invocation_ensure_terminal_1")
	if err != nil {
		t.Fatalf("list provider completion evidence: %v", err)
	}
	if len(evidence) != 1 {
		t.Fatalf("expected exactly one terminal evidence row, got %d", len(evidence))
	}
	if evidence[0].EvidenceKind != ProviderCompletionEvidenceKindStreamObservation || evidence[0].EvidenceID != first.EvidenceID {
		t.Fatalf("expected original stream evidence to remain authoritative, got %#v", evidence[0])
	}
}

func TestStoreEnsureTerminalProviderCompletionEvidenceRejectsUnknownEvidenceSource(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:work_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeServerResponse,
		SessionRef:     "session_run_1_work_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := activateProviderInvocationForTest(t, s, ctx, ActivateProviderInvocationRequest{
		ProviderInvocationID: "invocation_unknown_terminal_source",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_run_1_work_intake_1",
		ProviderSessionID:    "ses_unknown_terminal_source",
		CompletionMode:       ProviderCompletionModeServerResponse,
		IssuedAt:             "2026-03-01T12:00:02Z",
		CreatedAt:            "2026-03-01T12:00:02Z",
		OpenedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("activate provider invocation: %v", err)
	}

	if _, err := s.EnsureTerminalProviderCompletionEvidence(ctx, EnsureTerminalProviderCompletionEvidenceRequest{
		ProviderInvocationID: "invocation_unknown_terminal_source",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		State:                "failed",
		ReasonCode:           "provider_incomplete_turn",
		FailureText:          "provider terminal response was not clean",
		EvidenceSource:       "unknown",
		CreatedAt:            "2026-03-01T12:00:04Z",
	}); err == nil {
		t.Fatal("expected unknown terminal evidence source to fail")
	}
}

func TestStoreActivateProviderInvocationConfirmsDurableCommitError(t *testing.T) {
	connector := testutil.NewSQLiteCommitFaultConnector(":memory:")
	db := connector.OpenDB()
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	s.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 4, 0, time.UTC) }
	ctx := context.Background()
	const attemptID = "run_1:work_intake:activation_commit_ambiguous"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeServerResponse,
		SessionRef:     "session_activation_commit_ambiguous",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	claim, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: "worker_activation_commit_ambiguous",
		LeaseUntil: "2026-03-01T12:10:00Z",
		ClaimedAt:  "2026-03-01T12:00:02Z",
	})
	if err != nil || !acquired {
		t.Fatalf("claim provider attempt: acquired=%t err=%v", acquired, err)
	}

	connector.FailNextDurableCommit(func() {
		connector.FailNextQueryContaining("FROM provider_invocations WHERE provider_invocation_id")
	})
	activated, err := s.ActivateProviderInvocation(ctx, withBoundRequest(ActivateProviderInvocationRequest{
		ProviderInvocationID: "invocation_activation_commit_ambiguous",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_activation_commit_ambiguous",
		CompletionMode:       ProviderCompletionModeServerResponse,
		IssuedAt:             "2026-03-01T12:00:03Z",
		CreatedAt:            "2026-03-01T12:00:03Z",
		OpenedAt:             "2026-03-01T12:00:03Z",
		ExpectedLeaseOwner:   claim.LeaseOwner,
		ExpectedLeaseVersion: claim.LeaseVersion,
	}))
	if err != nil {
		t.Fatalf("confirm activation after durable commit error: %v", err)
	}
	if activated.ProviderInvocationID != "invocation_activation_commit_ambiguous" || activated.InvocationState != ProviderInvocationStateOpen {
		t.Fatalf("unexpected activated invocation: %#v", activated)
	}
	attempt, found, err := s.GetAttempt(ctx, attemptID)
	if err != nil || !found {
		t.Fatalf("read back activated attempt: found=%t err=%v", found, err)
	}
	if attempt.ActiveProviderInvocationID != activated.ProviderInvocationID || attempt.LeaseOwner != claim.LeaseOwner || attempt.LeaseVersion != claim.LeaseVersion {
		t.Fatalf("activation did not retain exact attempt authority: %#v", attempt)
	}
}

func TestStoreActivateProviderInvocationFencesConcurrentLeaseGenerations(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	db.SetMaxOpenConns(1)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	s.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 30, 0, time.UTC) }
	ctx := context.Background()
	const attemptID = "run_1:activation_generation:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: ExecutionModeProviderBacked, CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatalf("enqueue activation attempt: %v", err)
	}
	first, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "worker-first", LeaseUntil: "2026-03-01T12:00:00.000000001Z", ClaimedAt: "2026-03-01T12:00:00Z"})
	if err != nil || !acquired {
		t.Fatalf("claim first generation: claim=%#v acquired=%t err=%v", first, acquired, err)
	}
	second, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "worker-second", LeaseUntil: "2026-03-01T12:01:00Z", ClaimedAt: "2026-03-01T12:00:00.000000001Z"})
	if err != nil || !acquired || second.LeaseVersion != first.LeaseVersion+1 {
		t.Fatalf("claim second generation: claim=%#v acquired=%t err=%v", second, acquired, err)
	}

	requests := []ActivateProviderInvocationRequest{
		{ProviderInvocationID: "invocation_stale", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical_generation", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:00.000000001Z", OpenedAt: "2026-03-01T12:00:00.000000001Z", ExpectedLeaseOwner: first.LeaseOwner, ExpectedLeaseVersion: first.LeaseVersion},
		{ProviderInvocationID: "invocation_current", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical_generation", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:00.000000001Z", OpenedAt: "2026-03-01T12:00:00.000000001Z", ExpectedLeaseOwner: second.LeaseOwner, ExpectedLeaseVersion: second.LeaseVersion},
	}
	results := make(chan error, len(requests))
	var ready sync.WaitGroup
	ready.Add(len(requests))
	start := make(chan struct{})
	for _, request := range requests {
		go func(request ActivateProviderInvocationRequest) {
			ready.Done()
			<-start
			_, err := s.ActivateProviderInvocation(context.Background(), withBoundRequest(request))
			results <- err
		}(request)
	}
	ready.Wait()
	close(start)
	var successes int
	for range requests {
		if err := <-results; err == nil {
			successes++
		} else {
			requireProviderSettlementError(t, err, "activate_provider_invocation", "lease_lineage_mismatch")
		}
	}
	if successes != 1 {
		t.Fatalf("activation successes=%d, want one", successes)
	}
	attempt, found, err := s.GetAttempt(ctx, attemptID)
	if err != nil || !found {
		t.Fatalf("load activation attempt: found=%t err=%v", found, err)
	}
	if attempt.ActiveProviderInvocationID != "invocation_current" {
		t.Fatalf("stale generation activated provider invocation: %#v", attempt)
	}
}
