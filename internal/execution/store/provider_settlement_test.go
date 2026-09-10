package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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

func TestStorePersistProviderTerminalHandoffRequiresObservedLeaseAndExactLineage(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *sql.DB, *PersistProviderTerminalHandoffRequest)
		reason string
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
			reason: "lease_lineage_mismatch",
		},
		{
			name: "blank lease owner",
			mutate: func(_ *testing.T, _ *sql.DB, req *PersistProviderTerminalHandoffRequest) {
				req.ExpectedLeaseOwner = ""
			},
			reason: "lease_owner_required",
		},
		{
			name: "nonpositive lease version",
			mutate: func(_ *testing.T, _ *sql.DB, req *PersistProviderTerminalHandoffRequest) {
				req.ExpectedLeaseVersion = 0
			},
			reason: "lease_version_invalid",
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
			requireProviderSettlementError(t, err, "persist_provider_terminal_handoff", tc.reason)
			if count := countPhase2Table(t, db, "provider_terminal_handoffs"); count != 0 {
				t.Fatalf("rejected handoff persisted %d rows", count)
			}
		})
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
	if _, err := s.PersistProviderTerminalHandoff(context.Background(), changedLeaseLineage); err == nil {
		t.Fatal("expected changed handoff lease lineage to conflict")
	} else {
		requireProviderSettlementError(t, err, "persist_provider_terminal_handoff", "terminal_handoff_conflict")
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
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version) VALUES ('%s', 'run_1', 'decree_intake', %d, %d, 'running', 'king', 'agent_king', 1)`, nodeRunID, sequence, sequence))
		if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: nodeRunID, ExecutionMode: ExecutionModeProviderBacked, CompletionMode: ProviderCompletionModeReceiptRequired, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
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
		if _, err := s.ActivateProviderInvocation(ctx, ActivateProviderInvocationRequest{ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: nodeRunID, ProviderSessionID: sessionID, CompletionMode: ProviderCompletionModeReceiptRequired, CompletionCapability: "capability-" + invocationID, IssuedAt: "2026-03-01T12:00:02Z", CreatedAt: "2026-03-01T12:00:02Z", OpenedAt: "2026-03-01T12:00:02Z"}); err != nil {
			t.Fatalf("activate handoff fixture %q: %v", invocationID, err)
		}
		zero := 0
		if _, err := s.PersistProviderTerminalHandoff(ctx, PersistProviderTerminalHandoffRequest{ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: nodeRunID, ExpectedLeaseOwner: "worker-" + invocationID, ExpectedLeaseVersion: claim.LeaseVersion, StreamObservation: providerexecution.InvocationStreamObservation{ProviderSessionID: sessionID, ProcessExited: true, ProcessExitCode: &zero, StdoutDrained: true, StderrDrained: true}, PayloadJSON: fmt.Sprintf(`{"sequence":%d}`, sequence), CreatedAt: "2026-03-01T12:00:03Z"}); err != nil {
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

func TestStoreReceiptSuccessRequiresTerminalHandoff(t *testing.T) {
	t.Parallel()
	s, _, req := phase2ProviderSettlementFixture(t)
	_, err := s.SettleProviderCompletionAndTerminal(context.Background(), req)
	requireProviderSettlementError(t, err, "settle_provider_completion_and_terminal", "terminal_handoff_required")
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
			attachTerminalHandoffToSettlement(t, s, &req)
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
	attachTerminalHandoffToSettlement(t, s, &successReq)
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
	receipt, err := s.IngestProviderCompletionReceipt(ctx, IngestProviderCompletionReceiptRequest{ProviderInvocationID: invocationID, EntryName: "receipt.json", WireByteCount: 64, WireSHA256: strings.Repeat("a", 64), EntryType: "regular", ProtocolVersion: "v1", ObservedAttemptID: attemptID, ObservedInvocationID: invocationID, CompletionCapability: "capability-phase2", ValidationSucceeded: true, FinalText: "canonical response", StructuredPayloadJSON: `{}`, Disposition: ProviderCompletionReceiptDispositionAccepted, ObservedAt: "2026-03-01T12:00:03Z"})
	if err != nil {
		t.Fatalf("accept phase 2 fixture receipt: %v", err)
	}
	zero := 0
	observation := providerexecution.InvocationStreamObservation{ProviderSessionID: "ses_phase2", FinalTurnID: "turn_phase2", FinalStepID: "step_phase2", FinalSequence: 1, RawFinishReason: "stop", NormalizedFinishReason: "stop", CanonicalTerminalText: "canonical response", Tokens: providerexecution.TokenUsage{InputTokens: 10, OutputTokens: 4, TotalTokens: 14, ReasoningTokens: 2}, ProcessExited: true, ProcessExitCode: &zero, StdoutDrained: true, StderrDrained: true}
	return s, db, SettleProviderCompletionAndTerminalRequest{
		Settlement:           FinalizeProviderCompletionSettlementRequest{SettlementID: "settlement_phase2_success", ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", Outcome: ProviderCompletionOutcomeSuccess, PrimaryEvidenceID: receipt.AcceptedEvidence.EvidenceID, FinalText: "canonical response", StructuredPayloadJSON: `{}`, ProviderDiagnosticsJSON: `{}`, SettledAt: "2026-03-01T12:00:04Z"},
		StreamObservation:    &observation,
		ExpectedLeaseOwner:   "worker-phase2",
		ExpectedLeaseVersion: 1,
		TokenUsage:           &ProviderInvocationTokenUsage{RequiredRole: "king", Model: "github-copilot/gpt-5.4-mini", Variant: "low", CreatedAt: "2026-03-01T12:00:04Z"},
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
		expectedEvidenceCount = 3
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

func TestStoreIngestProviderCompletionReceiptAtomicallyAcceptsSafeEvidence(t *testing.T) {
	t.Parallel()
	s, db, req := providerReceiptIngestionFixture(t, "atomic")

	result, err := s.IngestProviderCompletionReceipt(context.Background(), req)
	if err != nil {
		t.Fatalf("ingest provider completion receipt: %v", err)
	}
	if result.Disposition != ProviderCompletionReceiptDispositionAccepted || result.Idempotent || result.ExactReplay {
		t.Fatalf("unexpected accepted ingestion result: %#v", result)
	}
	if result.Invocation.InvocationState != ProviderInvocationStateReceiptPersisted || result.Invocation.ReceiptSeenCount != 1 {
		t.Fatalf("accepted ingestion did not atomically update invocation: %#v", result.Invocation)
	}
	if result.AttemptEvidence.EvidenceKind != ProviderCompletionEvidenceKindCompletionReceiptAttempt || result.AcceptedEvidence.EvidenceKind != ProviderCompletionEvidenceKindCompletionReceipt {
		t.Fatalf("accepted ingestion did not persist both evidence rows: %#v", result)
	}
	if count := countPhase2Table(t, db, "provider_completion_evidence"); count != 2 {
		t.Fatalf("expected attempt and accepted evidence in one commit, got %d rows", count)
	}
	if strings.Contains(result.AttemptEvidence.PayloadJSON, req.CompletionCapability) {
		t.Fatal("receipt attempt telemetry persisted the raw completion capability")
	}
	var payload struct {
		EntryName               string          `json:"entry_name"`
		WireByteCount           int64           `json:"wire_byte_count"`
		WireSHA256              string          `json:"wire_sha256"`
		EntryType               string          `json:"entry_type"`
		CompletionCapabilitySHA string          `json:"completion_capability_sha256"`
		FinalText               string          `json:"final_text"`
		StructuredPayload       json.RawMessage `json:"structured_payload"`
		AcceptedEvidenceID      string          `json:"accepted_evidence_id"`
	}
	if err := json.Unmarshal([]byte(result.AttemptEvidence.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode accepted attempt evidence: %v", err)
	}
	if payload.EntryName != req.EntryName || payload.WireByteCount != req.WireByteCount || payload.WireSHA256 != req.WireSHA256 || payload.EntryType != req.EntryType {
		t.Fatalf("safe transport metadata was not preserved: %#v", payload)
	}
	if payload.CompletionCapabilitySHA != sha256Hex(req.CompletionCapability) || payload.FinalText != "canonical response" || string(payload.StructuredPayload) != `{"answer":true}` || payload.AcceptedEvidenceID != result.AcceptedEvidence.EvidenceID {
		t.Fatalf("accepted attempt evidence is incomplete: %#v", payload)
	}
}

func TestStoreIngestProviderCompletionReceiptRetainsOnlySafeRejectedMetadata(t *testing.T) {
	t.Parallel()
	s, _, req := providerReceiptIngestionFixture(t, "rejected")
	req.EntryName = "malformed.json"
	req.WireByteCount = 37
	req.WireSHA256 = strings.Repeat("b", 64)
	req.ValidationSucceeded = false
	req.FinalText = "raw malformed bytes must not survive"
	req.StructuredPayloadJSON = `{not-json`
	req.Disposition = ProviderCompletionReceiptDispositionRejected
	req.ReasonCode = "schema_validation_failed"
	req.ReasonText = "receipt is not valid JSON"

	result, err := s.IngestProviderCompletionReceipt(context.Background(), req)
	if err != nil {
		t.Fatalf("ingest rejected receipt: %v", err)
	}
	if result.Disposition != ProviderCompletionReceiptDispositionRejected || result.Invocation.ReceiptSeenCount != 1 || result.AcceptedEvidence.EvidenceID != "" {
		t.Fatalf("unexpected rejected ingestion result: %#v", result)
	}
	payload := result.AttemptEvidence.PayloadJSON
	if strings.Contains(payload, req.CompletionCapability) || strings.Contains(payload, "raw malformed bytes") || strings.Contains(payload, "not-json") {
		t.Fatalf("rejected telemetry retained unsafe receipt content: %s", payload)
	}
	for _, safe := range []string{`"entry_name":"malformed.json"`, `"wire_byte_count":37`, `"wire_sha256":"` + req.WireSHA256 + `"`, `"reason_code":"schema_validation_failed"`} {
		if !strings.Contains(payload, safe) {
			t.Fatalf("rejected telemetry omitted %s: %s", safe, payload)
		}
	}
	if !strings.Contains(payload, sha256Hex(req.CompletionCapability)) {
		t.Fatalf("rejected telemetry omitted the capability hash: %s", payload)
	}
}

func TestStoreIngestProviderCompletionReceiptExactReplayDoesNotIncrementCounters(t *testing.T) {
	t.Parallel()
	s, _, req := providerReceiptIngestionFixture(t, "replay")
	first, err := s.IngestProviderCompletionReceipt(context.Background(), req)
	if err != nil {
		t.Fatalf("first receipt ingestion: %v", err)
	}
	req.ObservedAt = "2026-03-01T12:00:09Z"
	replayed, err := s.IngestProviderCompletionReceipt(context.Background(), req)
	if err != nil {
		t.Fatalf("exact receipt replay: %v", err)
	}
	if !replayed.ExactReplay || !replayed.Idempotent || replayed.AttemptEvidence.EvidenceID != first.AttemptEvidence.EvidenceID || replayed.AcceptedEvidence.EvidenceID != first.AcceptedEvidence.EvidenceID {
		t.Fatalf("exact replay did not converge on deterministic evidence: first=%#v replay=%#v", first, replayed)
	}
	if replayed.Invocation.ReceiptSeenCount != 1 || replayed.Invocation.LastReceiptSeenAt != "2026-03-01T12:00:04Z" {
		t.Fatalf("exact replay changed receipt telemetry: %#v", replayed.Invocation)
	}
}

func TestStoreIngestProviderCompletionReceiptCanonicalOwnerConflictInvalidates(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*IngestProviderCompletionReceiptRequest)
		reasonCode string
	}{
		{name: "attempt", mutate: func(req *IngestProviderCompletionReceiptRequest) { req.ObservedAttemptID = "another-attempt" }, reasonCode: "attempt_lineage_mismatch"},
		{name: "invocation", mutate: func(req *IngestProviderCompletionReceiptRequest) { req.ObservedInvocationID = "another-invocation" }, reasonCode: "provider_invocation_claim_mismatch"},
		{name: "capability", mutate: func(req *IngestProviderCompletionReceiptRequest) { req.CompletionCapability = "another-capability" }, reasonCode: "completion_capability_mismatch"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, _, req := providerReceiptIngestionFixture(t, "canonical_owner_"+tc.name)
			tc.mutate(&req)
			result, err := s.IngestProviderCompletionReceipt(context.Background(), req)
			if err != nil {
				t.Fatalf("ingest cross-owner receipt: %v", err)
			}
			if result.AttemptEvidence.ProviderInvocationID != req.ProviderInvocationID || result.AttemptEvidence.AttemptID != result.Invocation.AttemptID {
				t.Fatalf("payload claims changed canonical evidence ownership: %#v", result.AttemptEvidence)
			}
			if result.Disposition != ProviderCompletionReceiptDispositionInvalidated || result.ReasonCode != tc.reasonCode {
				t.Fatalf("cross-owner receipt was not invalidated: %#v", result)
			}
			if result.Invocation.InvocationState != ProviderInvocationStateInvalidHandshake || result.Invocation.InvalidReason != result.ReasonCode || result.Invocation.ReceiptSeenCount != 1 {
				t.Fatalf("cross-owner conflict was not atomically applied: %#v", result.Invocation)
			}
		})
	}
}

func TestStoreIngestProviderCompletionReceiptAcceptedDuplicateIsIdempotent(t *testing.T) {
	t.Parallel()
	s, _, req := providerReceiptIngestionFixture(t, "accepted_duplicate")
	first, err := s.IngestProviderCompletionReceipt(context.Background(), req)
	if err != nil {
		t.Fatalf("accept first receipt: %v", err)
	}
	duplicate := req
	duplicate.EntryName = "duplicate.json"
	duplicate.WireSHA256 = strings.Repeat("d", 64)
	duplicate.ObservedAt = "2026-03-01T12:00:05Z"
	second, err := s.IngestProviderCompletionReceipt(context.Background(), duplicate)
	if err != nil {
		t.Fatalf("ingest accepted duplicate: %v", err)
	}
	if second.Disposition != ProviderCompletionReceiptDispositionIdempotentDuplicate || !second.Idempotent || second.ExactReplay {
		t.Fatalf("accepted duplicate was not idempotent: %#v", second)
	}
	if second.AcceptedEvidence.EvidenceID != first.AcceptedEvidence.EvidenceID || second.AttemptEvidence.EvidenceID == first.AttemptEvidence.EvidenceID || second.Invocation.ReceiptSeenCount != 2 {
		t.Fatalf("accepted duplicate did not preserve canonical evidence: first=%#v second=%#v", first, second)
	}
}

func TestStoreIngestProviderCompletionReceiptConflictingAcceptedReceiptInvalidates(t *testing.T) {
	t.Parallel()
	s, _, req := providerReceiptIngestionFixture(t, "accepted_conflict")
	first, err := s.IngestProviderCompletionReceipt(context.Background(), req)
	if err != nil {
		t.Fatalf("accept first receipt: %v", err)
	}
	conflict := req
	conflict.EntryName = "receipt-2.json"
	conflict.WireSHA256 = strings.Repeat("c", 64)
	conflict.FinalText = "conflicting response"
	conflict.ObservedAt = "2026-03-01T12:00:05Z"
	second, err := s.IngestProviderCompletionReceipt(context.Background(), conflict)
	if err != nil {
		t.Fatalf("ingest conflicting accepted receipt: %v", err)
	}
	if second.Disposition != ProviderCompletionReceiptDispositionInvalidated || second.ReasonCode != "completion_receipt_conflict" || second.Invocation.InvocationState != ProviderInvocationStateInvalidHandshake {
		t.Fatalf("conflicting accepted receipt did not invalidate the handshake: %#v", second)
	}
	if second.AcceptedEvidence.EvidenceID != "" || first.AcceptedEvidence.EvidenceID == "" || second.Invocation.ReceiptSeenCount != 2 {
		t.Fatalf("conflicting receipt changed accepted evidence incorrectly: first=%#v second=%#v", first, second)
	}
}

func TestStoreIngestProviderCompletionReceiptInfrastructureFailureRollsBackEverything(t *testing.T) {
	t.Parallel()
	s, db, req := providerReceiptIngestionFixture(t, "rollback")
	before, found, err := s.GetProviderInvocation(context.Background(), req.ProviderInvocationID)
	if err != nil || !found {
		t.Fatalf("load invocation before rollback test: found=%t err=%v", found, err)
	}
	if _, err := db.Exec(`CREATE TRIGGER phase3_ingestion_failure BEFORE INSERT ON provider_completion_evidence
		WHEN NEW.evidence_kind = 'completion_receipt_attempt'
		BEGIN SELECT RAISE(ABORT, 'injected ingestion failure'); END`); err != nil {
		t.Fatalf("install ingestion failure trigger: %v", err)
	}
	if _, err := s.IngestProviderCompletionReceipt(context.Background(), req); err == nil || !strings.Contains(err.Error(), "injected ingestion failure") {
		t.Fatalf("expected injected ingestion failure, got %v", err)
	}
	after, found, err := s.GetProviderInvocation(context.Background(), req.ProviderInvocationID)
	if err != nil || !found {
		t.Fatalf("load invocation after rollback test: found=%t err=%v", found, err)
	}
	if !reflect.DeepEqual(after, before) || countPhase2Table(t, db, "provider_completion_evidence") != 0 {
		t.Fatalf("infrastructure failure left partial ingestion state: before=%#v after=%#v", before, after)
	}
}

func TestStoreListProviderReceiptIngestionCandidatesPaginatesAndIncludesCleanupDebt(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	ctx := context.Background()

	create := func(sequence int, invocationID string, terminal bool) {
		t.Helper()
		nodeRunID := fmt.Sprintf("nr_receipt_candidate_%d", sequence)
		attemptID := fmt.Sprintf("run_1:receipt_candidate:%d", sequence)
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version) VALUES ('%s', 'run_1', 'decree_intake', %d, %d, 'running', 'king', 'agent_king', 1)`, nodeRunID, sequence+1, sequence+1))
		if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: nodeRunID, ExecutionMode: ExecutionModeProviderBacked, CompletionMode: ProviderCompletionModeReceiptRequired, CreatedAt: "2026-03-01T12:00:01Z"}); err != nil {
			t.Fatalf("enqueue candidate %q: %v", invocationID, err)
		}
		if _, err := s.ActivateProviderInvocation(ctx, ActivateProviderInvocationRequest{ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: nodeRunID, CompletionMode: ProviderCompletionModeReceiptRequired, CompletionCapability: "cap-" + invocationID, IssuedAt: "2026-03-01T12:00:02Z", CreatedAt: "2026-03-01T12:00:02Z", OpenedAt: "2026-03-01T12:00:02Z"}); err != nil {
			t.Fatalf("activate candidate %q: %v", invocationID, err)
		}
		if terminal {
			if _, err := db.Exec(`UPDATE attempt_executions SET state = 'committed_failure', terminal_observed_at = '2026-03-01T12:00:03Z', committed_at = '2026-03-01T12:00:03Z' WHERE attempt_id = ?`, attemptID); err != nil {
				t.Fatalf("terminalize candidate attempt %q: %v", invocationID, err)
			}
			if _, err := db.Exec(`UPDATE provider_invocations SET invocation_state = 'settled_failed', settled_at = '2026-03-01T12:00:03Z' WHERE provider_invocation_id = ?`, invocationID); err != nil {
				t.Fatalf("terminalize candidate invocation %q: %v", invocationID, err)
			}
		}
	}
	create(1, "candidate_a", false)
	create(2, "candidate_b", false)
	create(3, "candidate_c", true)
	mustExec(t, db, `UPDATE provider_invocations SET invocation_state = 'invalid_handshake', invalid_reason = 'bad claim' WHERE provider_invocation_id = 'candidate_b'`)

	first, err := s.ListProviderReceiptIngestionCandidates(ctx, "", 2)
	if err != nil {
		t.Fatalf("list first candidate page: %v", err)
	}
	if len(first) != 2 || first[0].ProviderInvocationID != "candidate_a" || first[1].ProviderInvocationID != "candidate_b" {
		t.Fatalf("unexpected first candidate page: %#v", first)
	}
	second, err := s.ListProviderReceiptIngestionCandidates(ctx, first[len(first)-1].ProviderInvocationID, 2)
	if err != nil {
		t.Fatalf("list second candidate page: %v", err)
	}
	if len(second) != 1 || second[0].ProviderInvocationID != "candidate_c" || second[0].InvocationState != ProviderInvocationStateSettledFailed {
		t.Fatalf("terminal cleanup debt missing from second page: %#v", second)
	}
}

func TestStoreMarkProviderReceiptInboxCleanedRequiresTerminalInvocation(t *testing.T) {
	t.Parallel()
	s, db, req := providerReceiptIngestionFixture(t, "cleanup")
	ctx := context.Background()
	if _, err := s.MarkProviderReceiptInboxCleaned(ctx, MarkProviderReceiptInboxCleanedRequest{ProviderInvocationID: req.ProviderInvocationID, CleanedAt: "2026-03-01T12:00:05Z"}); err == nil {
		t.Fatal("expected live invocation cleanup marker to be rejected")
	} else {
		requireProviderSettlementError(t, err, "mark_provider_receipt_inbox_cleaned", "invocation_not_terminal")
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET state = 'committed_failure', terminal_observed_at = '2026-03-01T12:00:05Z', committed_at = '2026-03-01T12:00:05Z' WHERE attempt_id = ?`, req.ObservedAttemptID); err != nil {
		t.Fatalf("terminalize cleanup attempt: %v", err)
	}
	if _, err := db.Exec(`UPDATE provider_invocations SET invocation_state = 'settled_failed', settled_at = '2026-03-01T12:00:05Z' WHERE provider_invocation_id = ?`, req.ProviderInvocationID); err != nil {
		t.Fatalf("terminalize cleanup invocation: %v", err)
	}
	marked, err := s.MarkProviderReceiptInboxCleaned(ctx, MarkProviderReceiptInboxCleanedRequest{ProviderInvocationID: req.ProviderInvocationID, CleanedAt: "2026-03-01T12:00:06Z"})
	if err != nil {
		t.Fatalf("mark terminal receipt inbox cleaned: %v", err)
	}
	if marked.ReceiptInboxCleanedAt != "2026-03-01T12:00:06Z" {
		t.Fatalf("cleanup marker did not persist: %#v", marked)
	}
	replayed, err := s.MarkProviderReceiptInboxCleaned(ctx, MarkProviderReceiptInboxCleanedRequest{ProviderInvocationID: req.ProviderInvocationID, CleanedAt: "2026-03-01T12:00:07Z"})
	if err != nil || replayed.ReceiptInboxCleanedAt != marked.ReceiptInboxCleanedAt {
		t.Fatalf("cleanup marker replay was not idempotent: invocation=%#v err=%v", replayed, err)
	}
	candidates, err := s.ListProviderReceiptIngestionCandidates(ctx, "", 10)
	if err != nil {
		t.Fatalf("list candidates after cleanup: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("cleaned terminal invocation remained candidate: %#v", candidates)
	}
}

func providerReceiptIngestionFixture(t *testing.T, suffix string) (*Store, *sql.DB, IngestProviderCompletionReceiptRequest) {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:receipt_ingestion:" + suffix
	invocationID := "invocation_receipt_ingestion_" + suffix
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: ExecutionModeProviderBacked, CompletionMode: ProviderCompletionModeReceiptRequired, CreatedAt: "2026-03-01T12:00:01Z"}); err != nil {
		t.Fatalf("enqueue receipt ingestion fixture: %v", err)
	}
	if _, err := s.ActivateProviderInvocation(ctx, ActivateProviderInvocationRequest{ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "logical_" + suffix, ProviderSessionID: "session_" + suffix, CompletionMode: ProviderCompletionModeReceiptRequired, CompletionCapability: "capability-" + suffix, IssuedAt: "2026-03-01T12:00:02Z", CreatedAt: "2026-03-01T12:00:02Z", OpenedAt: "2026-03-01T12:00:03Z"}); err != nil {
		t.Fatalf("activate receipt ingestion fixture: %v", err)
	}
	return s, db, IngestProviderCompletionReceiptRequest{
		ProviderInvocationID:  invocationID,
		EntryName:             "receipt.json",
		WireByteCount:         128,
		WireSHA256:            strings.Repeat("a", 64),
		EntryType:             "regular",
		ProtocolVersion:       "v1",
		ObservedAttemptID:     attemptID,
		ObservedInvocationID:  invocationID,
		CompletionCapability:  "capability-" + suffix,
		ValidationSucceeded:   true,
		FinalText:             " canonical response ",
		StructuredPayloadJSON: `{ "answer": true }`,
		Disposition:           ProviderCompletionReceiptDispositionAccepted,
		ObservedAt:            "2026-03-01T12:00:04Z",
	}
}
