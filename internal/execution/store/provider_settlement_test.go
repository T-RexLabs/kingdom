package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
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

func TestStoreCreateProviderInvocationRejectsAttemptLineageMismatch(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version) VALUES ('nr_2', 'run_1', 'royal_closure', 1, 1, 'running', 'king', 'agent_king', 1)`)
	s := New(db)
	const attemptID = "run_1:lineage:1"
	if err := s.EnqueueAttempt(context.Background(), EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: ExecutionModeProviderBacked, CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:01Z"}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}

	_, err := s.CreateProviderInvocation(context.Background(), CreateProviderInvocationRequest{ProviderInvocationID: "invocation_wrong_lineage", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_2", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:02Z"})
	if err == nil || !strings.Contains(err.Error(), "lineage") {
		t.Fatalf("expected attempt lineage rejection, got %v", err)
	}
	if count := countPhase2Table(t, db, "provider_invocations"); count != 0 {
		t.Fatalf("lineage rejection persisted %d provider invocations", count)
	}
	if _, err := db.Exec(`INSERT INTO provider_invocations (provider_invocation_id, attempt_id, run_id, node_run_id, invocation_state, completion_mode, created_at, updated_at) VALUES ('invocation_wrong_lineage_sql', ?, 'run_1', 'nr_2', 'issued', 'server_response', '2026-03-01T12:00:02Z', '2026-03-01T12:00:02Z')`, attemptID); err == nil {
		t.Fatal("expected SQL constraints to reject cross-lineage provider invocation")
	}
	if _, err := s.CreateProviderInvocation(context.Background(), CreateProviderInvocationRequest{ProviderInvocationID: "invocation_valid_lineage", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:02Z"}); err != nil {
		t.Fatalf("create valid provider invocation: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO token_usage (usage_id, run_id, node_run_id, attempt_id, provider_invocation_id, required_role, created_at) VALUES ('usage_wrong_lineage', 'run_1', 'nr_2', ?, 'invocation_valid_lineage', 'king', '2026-03-01T12:00:03Z')`, attemptID); err == nil {
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
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{ProviderInvocationID: "invocation_atomic_1", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical_atomic", CompletionMode: ProviderCompletionModeServerResponse, IssuedAt: "2026-03-01T12:00:02Z", CreatedAt: "2026-03-01T12:00:02Z"}); err != nil {
		t.Fatalf("create invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{ProviderInvocationID: "invocation_atomic_1", AttemptID: attemptID, ProviderSessionID: "ses_atomic", OpenedAt: "2026-03-01T12:00:03Z"}); err != nil {
		t.Fatalf("open invocation: %v", err)
	}
	if acquired, err := s.AcquireLease(ctx, AcquireLeaseRequest{AttemptID: attemptID, LeaseOwner: "worker_atomic", LeaseUntil: "2026-03-01T12:10:00Z", AcquiredAt: "2026-03-01T12:00:03Z", StateOnLease: "running"}); err != nil || !acquired {
		t.Fatalf("acquire settlement lease: acquired=%t err=%v", acquired, err)
	}
	zero := 0
	observation := providerexecution.InvocationStreamObservation{
		ProviderSessionID:      "ses_atomic",
		FinalTurnID:            "turn_atomic",
		FinalStepID:            "step_atomic",
		FinalSequence:          3,
		RawFinishReason:        "stop",
		NormalizedFinishReason: "stop",
		CanonicalTerminalText:  "canonical response",
		Tokens:                 providerexecution.TokenUsage{InputTokens: 10, OutputTokens: 4, TotalTokens: 14, ReasoningTokens: 2},
		ProcessExited:          true,
		ProcessExitCode:        &zero,
		StdoutDrained:          true,
		StderrDrained:          true,
	}
	settleReq := SettleProviderCompletionAndTerminalRequest{
		Settlement:           FinalizeProviderCompletionSettlementRequest{SettlementID: "settlement_atomic_1", ProviderInvocationID: "invocation_atomic_1", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", Outcome: ProviderCompletionOutcomeSuccess, FinalText: "canonical response", StructuredPayloadJSON: `{}`, ProviderDiagnosticsJSON: `{}`},
		StreamObservation:    &observation,
		ExpectedLeaseOwner:   "worker_atomic",
		ExpectedLeaseVersion: 1,
		TokenUsage:           &ProviderInvocationTokenUsage{RequiredRole: "king", Model: "github-copilot/gpt-5.4-mini", Variant: "low"},
		Terminal:             CommitTerminalRequest{OutboxID: "outbox_atomic_1", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", TerminalState: "committed_success", PayloadJSON: `{"output_text":"canonical response"}`, ProviderSessionID: "ses_atomic"},
	}
	settlement, err := s.SettleProviderCompletionAndTerminal(ctx, settleReq)
	if err != nil {
		t.Fatalf("atomic settlement: %v", err)
	}
	if settlement.PrimaryEvidenceID != TerminalProviderCompletionEvidenceID("invocation_atomic_1") {
		t.Fatalf("unexpected primary evidence: %#v", settlement)
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
		RequiredRole: "king",
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
	conflictingTokenUsage.TokenUsage = &ProviderInvocationTokenUsage{RequiredRole: "king", Model: "changed-model", Variant: "low"}
	_, err = s.SettleProviderCompletionAndTerminal(ctx, conflictingTokenUsage)
	var tokenConflict *ProviderSettlementError
	if !errors.As(err, &tokenConflict) || tokenConflict.Reason != "token_usage_conflict" {
		t.Fatalf("expected contradictory token replay conflict, got %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{ProviderInvocationID: "invocation_atomic_stale", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical_atomic", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:05Z"}); err != nil {
		t.Fatalf("create stale invocation: %v", err)
	}
	_, err = s.SettleProviderCompletionAndTerminal(ctx, SettleProviderCompletionAndTerminalRequest{
		Settlement:        FinalizeProviderCompletionSettlementRequest{SettlementID: "settlement_atomic_stale", ProviderInvocationID: "invocation_atomic_stale", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", Outcome: ProviderCompletionOutcomeFailed, PrimaryEvidenceID: "unused", ProviderDiagnosticsJSON: `{}`, SettledAt: "2026-03-01T12:00:06Z"},
		StreamObservation: &observation,
		TokenUsage:        &ProviderInvocationTokenUsage{RequiredRole: "king", CreatedAt: "2026-03-01T12:00:06Z"},
		Terminal:          CommitTerminalRequest{OutboxID: "outbox_atomic_stale", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", TerminalState: "committed_failure", PayloadJSON: `{}`, CommittedAt: "2026-03-01T12:00:06Z"},
	})
	var settlementErr *ProviderSettlementError
	if !errors.As(err, &settlementErr) || settlementErr.Reason != "invocation_not_active" {
		t.Fatalf("expected stale invocation to be rejected, got %v", err)
	}
}

func TestStoreSettleProviderCompletionAndTerminalRejectsInvalidLeaseWithoutPartialWrites(t *testing.T) {
	tests := []struct {
		name      string
		owner     string
		version   int64
		expiresAt string
		settledAt string
		reason    string
	}{
		{name: "blank owner", owner: "", version: 1, reason: "lease_owner_required"},
		{name: "zero version", owner: "worker-a", version: 0, reason: "lease_version_invalid"},
		{name: "stale owner", owner: "worker-b", version: 1, reason: "lease_lineage_mismatch"},
		{name: "stale version", owner: "worker-a", version: 2, reason: "lease_lineage_mismatch"},
		{name: "expired lease", owner: "worker-a", version: 1, expiresAt: "2026-03-01T12:00:03Z", reason: "lease_expired"},
		{name: "backdated after lease expiry", owner: "worker-a", version: 1, expiresAt: "2026-03-01T12:00:03Z", settledAt: "2026-03-01T12:00:02Z", reason: "lease_expired"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, db, req := atomicProviderSettlementFixture(t)
			if tc.expiresAt != "" {
				if _, err := db.Exec(`UPDATE attempt_executions SET lease_expires_at = ? WHERE attempt_id = ?`, tc.expiresAt, req.Settlement.AttemptID); err != nil {
					t.Fatalf("expire fixture lease: %v", err)
				}
			}
			req.ExpectedLeaseOwner = tc.owner
			req.ExpectedLeaseVersion = tc.version
			if tc.settledAt != "" {
				req.Settlement.SettledAt = tc.settledAt
			}
			_, err := s.SettleProviderCompletionAndTerminal(context.Background(), req)
			var settlementErr *ProviderSettlementError
			if !errors.As(err, &settlementErr) || settlementErr.Reason != tc.reason {
				t.Fatalf("expected %s rejection, got %v", tc.reason, err)
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
			name: "invocation terminal update and capability revocation",
			trigger: `CREATE TRIGGER phase2_injected_failure BEFORE UPDATE OF invocation_state, completion_capability_revoked_at ON provider_invocations
				WHEN NEW.invocation_state = 'settled_success' AND NEW.completion_capability_revoked_at IS NOT NULL
				BEGIN SELECT RAISE(ABORT, 'injected failure: invocation terminal update and capability revocation'); END`,
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
			before := capturePhase2ProviderSettlementState(t, s, db, req.Settlement.AttemptID, req.Settlement.ProviderInvocationID)
			if before.Attempt.LeaseOwner != req.ExpectedLeaseOwner || before.Attempt.LeaseExpiresAt == "" || before.Attempt.LeaseVersion != req.ExpectedLeaseVersion {
				t.Fatalf("fixture is missing original lease lineage: %#v", before.Attempt)
			}
			if before.Invocation.CompletionCapabilityRevokedAt != "" {
				t.Fatalf("fixture capability was already revoked: %#v", before.Invocation)
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

func TestStoreSettleProviderCompletionAndTerminalCancellationRacesSuccess(t *testing.T) {
	s, db, successReq := phase2ProviderSettlementFixture(t)
	db.SetMaxOpenConns(1)

	cancelReq := successReq
	cancelReq.Settlement = FinalizeProviderCompletionSettlementRequest{
		SettlementID:            "settlement_phase2_cancelled",
		ProviderInvocationID:    successReq.Settlement.ProviderInvocationID,
		AttemptID:               successReq.Settlement.AttemptID,
		RunID:                   successReq.Settlement.RunID,
		NodeRunID:               successReq.Settlement.NodeRunID,
		Outcome:                 ProviderCompletionOutcomeCancelled,
		ProviderDiagnosticsJSON: `{}`,
		FailureReasonCode:       "run_cancelled",
		FailureReasonText:       "run cancellation won terminal authority",
		SettledAt:               successReq.Settlement.SettledAt,
	}
	cancelReq.Terminal = CommitTerminalRequest{
		OutboxID:           "outbox_phase2_cancelled",
		AttemptID:          successReq.Terminal.AttemptID,
		RunID:              successReq.Terminal.RunID,
		NodeRunID:          successReq.Terminal.NodeRunID,
		TerminalState:      "committed_cancelled",
		PayloadJSON:        `{"cancelled":true}`,
		CommittedAt:        successReq.Terminal.CommittedAt,
		TerminalReasonCode: "run_cancelled",
		FailureCode:        "run_cancelled",
		FailureText:        "run cancellation won terminal authority",
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
	launch("cancellation", cancelReq)
	<-ready
	<-ready
	close(start)

	completed := map[string]callResult{}
	for range 2 {
		result := <-results
		completed[result.name] = result
	}
	successResult := completed["success"]
	cancelResult := completed["cancellation"]
	if (successResult.err == nil) == (cancelResult.err == nil) {
		t.Fatalf("expected exactly one terminal authority winner, success=%v cancellation=%v", successResult.err, cancelResult.err)
	}

	winnerReq, loserReq := successReq, cancelReq
	winnerResult, loserResult := successResult, cancelResult
	if cancelResult.err == nil {
		winnerReq, loserReq = cancelReq, successReq
		winnerResult, loserResult = cancelResult, successResult
	}
	requireProviderSettlementError(t, loserResult.err, "finalize_provider_completion_settlement", "settlement_conflict")
	if replayed, err := s.SettleProviderCompletionAndTerminal(context.Background(), winnerReq); err != nil || replayed != winnerResult.settlement {
		t.Fatalf("winner exact replay did not converge: settlement=%#v err=%v", replayed, err)
	}
	if _, err := s.SettleProviderCompletionAndTerminal(context.Background(), loserReq); err == nil {
		t.Fatal("expected losing terminal outcome replay to remain conflicting")
	} else {
		requireProviderSettlementError(t, err, "finalize_provider_completion_settlement", "settlement_conflict")
	}

	assertPhase2TerminalWinner(t, s, db, winnerReq, winnerResult.settlement)
	loserSettlementCount := countPhase2Rows(t, db, "provider_completion_settlements", "settlement_id", loserReq.Settlement.SettlementID)
	loserOutboxCount := countPhase2Rows(t, db, "attempt_outbox", "outbox_id", loserReq.Terminal.OutboxID)
	if loserSettlementCount != 0 || loserOutboxCount != 0 {
		t.Fatalf("losing terminal outcome left partial rows: settlements=%d outbox=%d", loserSettlementCount, loserOutboxCount)
	}
}

func TestStoreSettleProviderCompletionAndTerminalRacesInvocationReplacement(t *testing.T) {
	s, db, completionReq := atomicProviderSettlementFixture(t)
	db.SetMaxOpenConns(1)
	replacementReq := ActivateProviderInvocationRequest{
		ProviderInvocationID:         "invocation_phase2_replacement",
		AttemptID:                    completionReq.Settlement.AttemptID,
		RunID:                        completionReq.Settlement.RunID,
		NodeRunID:                    completionReq.Settlement.NodeRunID,
		LogicalSessionID:             "logical_atomic",
		ProviderSessionID:            "ses_phase2_replacement",
		CompletionMode:               ProviderCompletionModeServerResponse,
		IssuedAt:                     "2026-03-01T12:00:04Z",
		CreatedAt:                    "2026-03-01T12:00:04Z",
		OpenedAt:                     "2026-03-01T12:00:04Z",
		ReplacesProviderInvocationID: completionReq.Settlement.ProviderInvocationID,
	}

	type raceResult struct {
		operation  string
		settlement ProviderCompletionSettlement
		invocation ProviderInvocation
		err        error
	}
	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	results := make(chan raceResult, 2)
	go func() {
		ready <- struct{}{}
		<-start
		settlement, err := s.SettleProviderCompletionAndTerminal(context.Background(), completionReq)
		results <- raceResult{operation: "completion", settlement: settlement, err: err}
	}()
	go func() {
		ready <- struct{}{}
		<-start
		invocation, err := s.ActivateProviderInvocation(context.Background(), replacementReq)
		results <- raceResult{operation: "replacement", invocation: invocation, err: err}
	}()
	<-ready
	<-ready
	close(start)

	completed := map[string]raceResult{}
	for range 2 {
		result := <-results
		completed[result.operation] = result
	}
	completionResult := completed["completion"]
	replacementResult := completed["replacement"]
	if (completionResult.err == nil) == (replacementResult.err == nil) {
		t.Fatalf("expected exactly one authority winner, completion=%v replacement=%v", completionResult.err, replacementResult.err)
	}

	if completionResult.err == nil {
		requireProviderSettlementError(t, replacementResult.err, "activate_provider_invocation", "invocation_not_supersedable")
		if _, err := s.ActivateProviderInvocation(context.Background(), replacementReq); err == nil {
			t.Fatal("expected replacement retry after completion to remain fenced")
		} else {
			requireProviderSettlementError(t, err, "activate_provider_invocation", "invocation_not_supersedable")
		}
		assertPhase2TerminalWinner(t, s, db, completionReq, completionResult.settlement)
		if _, found, err := s.GetProviderInvocation(context.Background(), replacementReq.ProviderInvocationID); err != nil {
			t.Fatalf("load rejected replacement invocation: %v", err)
		} else if found {
			t.Fatal("completion winner left a partial replacement invocation")
		}
		return
	}

	requireProviderSettlementError(t, completionResult.err, "settle_provider_completion_and_terminal", "invocation_not_active")
	if _, err := s.SettleProviderCompletionAndTerminal(context.Background(), completionReq); err == nil {
		t.Fatal("expected old completion retry after replacement to remain fenced")
	} else {
		requireProviderSettlementError(t, err, "settle_provider_completion_and_terminal", "invocation_not_active")
	}
	attempt, found, err := s.GetAttempt(context.Background(), completionReq.Settlement.AttemptID)
	if err != nil || !found {
		t.Fatalf("load replacement-winning attempt: found=%t err=%v", found, err)
	}
	if attempt.State != "running" || attempt.ActiveProviderInvocationID != replacementReq.ProviderInvocationID || attempt.CanonicalSettlementID != "" || attempt.LeaseOwner != completionReq.ExpectedLeaseOwner || attempt.LeaseExpiresAt == "" || attempt.LeaseVersion != completionReq.ExpectedLeaseVersion {
		t.Fatalf("replacement winner left mixed attempt truth: %#v", attempt)
	}
	oldInvocation, found, err := s.GetProviderInvocation(context.Background(), completionReq.Settlement.ProviderInvocationID)
	if err != nil || !found {
		t.Fatalf("load superseded invocation: found=%t err=%v", found, err)
	}
	if oldInvocation.InvocationState != ProviderInvocationStateSuperseded || oldInvocation.SupersededByInvocationID != replacementReq.ProviderInvocationID {
		t.Fatalf("old invocation was not fully superseded: %#v", oldInvocation)
	}
	newInvocation, found, err := s.GetProviderInvocation(context.Background(), replacementReq.ProviderInvocationID)
	if err != nil || !found {
		t.Fatalf("load replacement invocation: found=%t err=%v", found, err)
	}
	if newInvocation.InvocationState != ProviderInvocationStateOpen || newInvocation.ProviderSessionID != replacementReq.ProviderSessionID {
		t.Fatalf("replacement invocation was not fully activated: %#v", newInvocation)
	}
	for _, table := range []string{"provider_completion_evidence", "provider_completion_settlements", "attempt_event_log", "attempt_outbox", "token_usage"} {
		if count := countPhase2Table(t, db, table); count != 0 {
			t.Fatalf("replacement winner left partial old-completion rows in %s: %d", table, count)
		}
	}
}

func atomicProviderSettlementFixture(t *testing.T) (*Store, *sql.DB, SettleProviderCompletionAndTerminalRequest) {
	t.Helper()
	db := openTestDB(t)
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
	if _, err := s.ActivateProviderInvocation(ctx, ActivateProviderInvocationRequest{ProviderInvocationID: "invocation_atomic_lease_rejection", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical_atomic", CompletionMode: ProviderCompletionModeServerResponse, ProviderSessionID: "ses_atomic", IssuedAt: "2026-03-01T12:00:02Z", CreatedAt: "2026-03-01T12:00:02Z", OpenedAt: "2026-03-01T12:00:02Z"}); err != nil {
		t.Fatalf("activate fixture invocation: %v", err)
	}
	zero := 0
	observation := providerexecution.InvocationStreamObservation{ProviderSessionID: "ses_atomic", FinalTurnID: "turn_atomic", FinalStepID: "step_atomic", FinalSequence: 1, RawFinishReason: "stop", NormalizedFinishReason: "stop", CanonicalTerminalText: "canonical response", ProcessExited: true, ProcessExitCode: &zero, StdoutDrained: true, StderrDrained: true}
	return s, db, SettleProviderCompletionAndTerminalRequest{
		Settlement:           FinalizeProviderCompletionSettlementRequest{SettlementID: "settlement_atomic_lease_rejection", ProviderInvocationID: "invocation_atomic_lease_rejection", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", Outcome: ProviderCompletionOutcomeSuccess, FinalText: "canonical response", StructuredPayloadJSON: `{}`, ProviderDiagnosticsJSON: `{}`, SettledAt: "2026-03-01T12:00:04Z"},
		StreamObservation:    &observation,
		ExpectedLeaseOwner:   "worker-a",
		ExpectedLeaseVersion: 1,
		TokenUsage:           &ProviderInvocationTokenUsage{RequiredRole: "king", CreatedAt: "2026-03-01T12:00:04Z"},
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
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	s.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 4, 0, time.UTC) }
	ctx := context.Background()
	const attemptID = "run_1:phase2_settlement:1"
	const invocationID = "invocation_phase2_settlement"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: ExecutionModeProviderBacked, CompletionMode: ProviderCompletionModeReceiptRequired, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatalf("enqueue phase 2 fixture attempt: %v", err)
	}
	claim, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "worker-phase2", LeaseUntil: "2026-03-01T12:10:00Z", ClaimedAt: "2026-03-01T12:00:01Z"})
	if err != nil || !acquired || claim.LeaseVersion != 1 {
		t.Fatalf("claim phase 2 fixture attempt: claim=%#v acquired=%t err=%v", claim, acquired, err)
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:00:02Z' WHERE attempt_id = ?`, attemptID); err != nil {
		t.Fatalf("mark phase 2 fixture attempt running: %v", err)
	}
	if _, err := s.ActivateProviderInvocation(ctx, ActivateProviderInvocationRequest{ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical_phase2", CompletionMode: ProviderCompletionModeReceiptRequired, CompletionCapability: "capability-phase2", ProviderSessionID: "ses_phase2", IssuedAt: "2026-03-01T12:00:02Z", CreatedAt: "2026-03-01T12:00:02Z", OpenedAt: "2026-03-01T12:00:02Z"}); err != nil {
		t.Fatalf("activate phase 2 fixture invocation: %v", err)
	}
	receipt, err := s.AcceptProviderCompletionReceipt(ctx, AcceptProviderCompletionReceiptRequest{EvidenceID: "receipt_phase2_settlement", ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ProtocolVersion: "v1", CompletionCapability: "capability-phase2", FinalText: "canonical response", StructuredPayloadJSON: `{}`, ReceivedAt: "2026-03-01T12:00:03Z"})
	if err != nil {
		t.Fatalf("accept phase 2 fixture receipt: %v", err)
	}
	zero := 0
	observation := providerexecution.InvocationStreamObservation{ProviderSessionID: "ses_phase2", FinalTurnID: "turn_phase2", FinalStepID: "step_phase2", FinalSequence: 1, RawFinishReason: "stop", NormalizedFinishReason: "stop", CanonicalTerminalText: "canonical response", Tokens: providerexecution.TokenUsage{InputTokens: 10, OutputTokens: 4, TotalTokens: 14, ReasoningTokens: 2}, ProcessExited: true, ProcessExitCode: &zero, StdoutDrained: true, StderrDrained: true}
	return s, db, SettleProviderCompletionAndTerminalRequest{
		Settlement:           FinalizeProviderCompletionSettlementRequest{SettlementID: "settlement_phase2_success", ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", Outcome: ProviderCompletionOutcomeSuccess, PrimaryEvidenceID: receipt.Evidence.EvidenceID, FinalText: "canonical response", StructuredPayloadJSON: `{}`, ProviderDiagnosticsJSON: `{}`, SettledAt: "2026-03-01T12:00:04Z"},
		StreamObservation:    &observation,
		ExpectedLeaseOwner:   "worker-phase2",
		ExpectedLeaseVersion: 1,
		TokenUsage:           &ProviderInvocationTokenUsage{RequiredRole: "king", Model: "github-copilot/gpt-5.4-mini", Variant: "low", CreatedAt: "2026-03-01T12:00:04Z"},
		Terminal:             CommitTerminalRequest{OutboxID: "outbox_phase2_success", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", TerminalState: "committed_success", PayloadJSON: `{"output_text":"canonical response"}`, CommittedAt: "2026-03-01T12:00:04Z", ProviderSessionID: "ses_phase2"},
	}
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
	if settlement.Outcome == ProviderCompletionOutcomeCancelled {
		expectedInvocationState = ProviderInvocationStateSettledCancelled
	}
	capabilityRevocationValid := invocation.CompletionMode != ProviderCompletionModeReceiptRequired || invocation.CompletionCapabilityRevokedAt != ""
	if invocation.InvocationState != expectedInvocationState || !capabilityRevocationValid || invocation.SupersededByInvocationID != "" {
		t.Fatalf("terminal winner left mixed invocation truth: %#v", invocation)
	}
	expectedEvidenceCount := 1
	if invocation.CompletionMode == ProviderCompletionModeReceiptRequired {
		expectedEvidenceCount = 2
	}
	for table, expected := range map[string]int{
		"provider_completion_evidence":    expectedEvidenceCount,
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

func TestStoreEnsureTerminalProviderCompletionEvidenceRejectsChangedReplay(t *testing.T) {
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
		FailureText:          "provider exited without an accepted kingdom_complete_attempt receipt",
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
