package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"kingdom/internal/testutil"
)

func TestStoreAcknowledgeProviderProcessGuardFencesActiveLeaseAndReplaysExactly(t *testing.T) {
	t.Parallel()
	s, db, req := providerProcessGuardFixture(t)
	ack := providerProcessGuardAcknowledgement(req, "guard_fenced", 1)

	stale := ack
	stale.ExpectedLeaseOwner = "stale-worker"
	if _, err := s.AcknowledgeProviderProcessGuard(context.Background(), stale); err == nil {
		t.Fatal("expected stale lease owner to be fenced")
	} else {
		requireProviderSettlementError(t, err, "acknowledge_provider_process_guard", "lease_lineage_mismatch")
	}
	if count := countPhase2Table(t, db, "provider_process_guards"); count != 0 {
		t.Fatalf("stale acknowledgement persisted %d guards", count)
	}

	first, err := s.AcknowledgeProviderProcessGuard(context.Background(), ack)
	if err != nil || first.Idempotent || first.Guard.Lifecycle != ProviderProcessGuardLifecycleAcknowledged {
		t.Fatalf("unexpected first acknowledgement: result=%#v err=%v", first, err)
	}
	replay, err := s.AcknowledgeProviderProcessGuard(context.Background(), ack)
	if err != nil || !replay.Idempotent || replay.Guard != first.Guard {
		t.Fatalf("exact acknowledgement replay did not converge: first=%#v replay=%#v err=%v", first, replay, err)
	}

	conflict := ack
	conflict.ChildIdentityToken = "different-child-start"
	if _, err := s.AcknowledgeProviderProcessGuard(context.Background(), conflict); err == nil {
		t.Fatal("expected changed acknowledgement to conflict")
	} else {
		requireProviderSettlementError(t, err, "acknowledge_provider_process_guard", "guard_conflict")
	}
}

func TestStoreAcknowledgeProviderProcessGuardConfirmsDurableCommitError(t *testing.T) {
	connector := testutil.NewSQLiteCommitFaultConnector(":memory:")
	db := connector.OpenDB()
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	s, db, settlement := providerProcessGuardFixtureWithDB(t, db)
	ack := providerProcessGuardAcknowledgement(settlement, "guard_commit_then_error", 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connector.FailNextDurableCommit(cancel)
	result, err := s.AcknowledgeProviderProcessGuard(ctx, ack)
	if err != nil {
		t.Fatalf("confirm exact acknowledgement after durable commit error: %v", err)
	}
	if ctx.Err() != context.Canceled {
		t.Fatal("expected the commit fault to cancel the request before readback")
	}
	if result.Idempotent || result.Guard.Lifecycle != ProviderProcessGuardLifecycleAcknowledged {
		t.Fatalf("unexpected acknowledged result after commit error: %#v", result)
	}
	normalized, err := normalizeAcknowledgeProviderProcessGuardRequest(ack)
	if err != nil {
		t.Fatalf("normalize acknowledgement: %v", err)
	}
	if !sameProviderProcessGuardAcknowledgement(result.Guard, providerProcessGuardFromAcknowledgement(normalized)) {
		t.Fatalf("readback returned a different acknowledgement lineage: %#v", result.Guard)
	}
	connector.FailNextDurableCommit(nil)
	replay, err := s.AcknowledgeProviderProcessGuard(context.Background(), ack)
	if err != nil || !replay.Idempotent || replay.Guard != result.Guard {
		t.Fatalf("confirm exact acknowledgement replay after durable commit error: replay=%#v err=%v", replay, err)
	}
}

func TestStoreAcknowledgeProviderProcessGuardRejectsMismatchedDurableCommitReadback(t *testing.T) {
	connector := testutil.NewSQLiteCommitFaultConnector(":memory:")
	db := connector.OpenDB()
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	s, db, settlement := providerProcessGuardFixtureWithDB(t, db)
	ack := providerProcessGuardAcknowledgement(settlement, "guard_mismatched_commit", 1)
	if _, err := db.Exec(`CREATE TRIGGER alter_acknowledged_guard_lease AFTER INSERT ON provider_process_guards WHEN NEW.guard_id = 'guard_mismatched_commit' BEGIN UPDATE provider_process_guards SET lease_version = lease_version + 1 WHERE guard_id = NEW.guard_id; END`); err != nil {
		t.Fatalf("create acknowledgement lineage trigger: %v", err)
	}
	connector.FailNextDurableCommit(nil)

	_, err := s.AcknowledgeProviderProcessGuard(context.Background(), ack)
	if err == nil || !errors.Is(err, testutil.ErrDurableCommit) {
		t.Fatalf("expected the mismatched durable row to fail closed with the commit error, got %v", err)
	}
}

func TestStoreProviderProcessGuardQuiescenceIsIdempotentAndRejectsConflicts(t *testing.T) {
	t.Parallel()
	s, _, req := providerProcessGuardFixture(t)
	ack := providerProcessGuardAcknowledgement(req, "guard_quiescence", 1)
	if _, err := s.AcknowledgeProviderProcessGuard(context.Background(), ack); err != nil {
		t.Fatalf("acknowledge guard: %v", err)
	}
	proof := providerProcessGuardQuiescenceProof(ack)
	first, err := s.IngestProviderProcessGuardQuiescence(context.Background(), proof)
	if err != nil || first.Idempotent || first.Guard.Lifecycle != ProviderProcessGuardLifecycleQuiescent {
		t.Fatalf("unexpected first quiescence proof: result=%#v err=%v", first, err)
	}
	replay, err := s.IngestProviderProcessGuardQuiescence(context.Background(), proof)
	if err != nil || !replay.Idempotent || replay.Guard != first.Guard {
		t.Fatalf("exact quiescence replay did not converge: first=%#v replay=%#v err=%v", first, replay, err)
	}
	observedAgain := proof
	observedAgain.ExitedAt = "2026-03-01T12:00:05Z"
	observedAgain.QuiescentAt = "2026-03-01T12:00:05Z"
	replay, err = s.IngestProviderProcessGuardQuiescence(context.Background(), observedAgain)
	if err != nil || !replay.Idempotent || replay.Guard != first.Guard {
		t.Fatalf("quiescence replay with a later observation timestamp did not converge: first=%#v replay=%#v err=%v", first, replay, err)
	}

	conflict := proof
	conflict.TerminationForced = true
	conflict.EvidenceJSON = providerProcessGuardEvidenceJSON(conflict, true)
	conflict.EvidenceSHA256 = sha256Hex(conflict.EvidenceJSON)
	if _, err := s.IngestProviderProcessGuardQuiescence(context.Background(), conflict); err == nil {
		t.Fatal("expected changed quiescence proof to conflict")
	} else {
		requireProviderSettlementError(t, err, "ingest_provider_process_guard_quiescence", "quiescence_conflict")
	}

	invalidCapability := proof
	invalidCapability.QuiescenceCapability = "wrong-capability"
	if _, err := s.IngestProviderProcessGuardQuiescence(context.Background(), invalidCapability); err == nil {
		t.Fatal("expected capability mismatch to be rejected")
	} else {
		requireProviderSettlementError(t, err, "ingest_provider_process_guard_quiescence", "quiescence_capability_mismatch")
	}
}

func TestStoreProviderQuiescenceResumesCancellationPhases(t *testing.T) {
	for _, phase := range []string{"cancellation_requested", "cancellation_dispatched", "stop_retry_requested", "stop_retry_dispatched", "awaiting_quiescence", "guardian_evidence_lost", "guardian_evidence_recovery_requested"} {
		for _, replay := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/replay=%t", phase, replay), func(t *testing.T) {
				t.Parallel()
				s, db, req := providerProcessGuardFixture(t)
				ctx := context.Background()
				ack := providerProcessGuardAcknowledgement(req, "guard_cancel_phase", 1)
				if _, err := s.AcknowledgeProviderProcessGuard(ctx, ack); err != nil {
					t.Fatal(err)
				}
				proof := providerProcessGuardQuiescenceProof(ack)
				if replay {
					if _, err := s.IngestProviderProcessGuardQuiescence(ctx, proof); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := db.Exec(`UPDATE attempt_executions SET state='committed_cancelled', lease_owner='', lease_expires_at=NULL WHERE attempt_id=?`, ack.AttemptID); err != nil {
					t.Fatal(err)
				}
				if err := s.QueueAttemptCancel(ctx, QueueAttemptCancelRequest{
					CancelOutboxID: "cancel_phase", AttemptID: ack.AttemptID, RunID: ack.RunID, NodeRunID: ack.NodeRunID,
					RequestedAt: proof.QuiescentAt, CreatedAt: proof.QuiescentAt,
				}); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`UPDATE attempt_cancel_outbox SET handoff_state=? WHERE cancel_outbox_id='cancel_phase'`, phase); err != nil {
					t.Fatal(err)
				}
				if _, err := s.IngestProviderProcessGuardQuiescence(ctx, proof); err != nil {
					t.Fatal(err)
				}
				var got string
				if err := db.QueryRow(`SELECT handoff_state FROM attempt_cancel_outbox WHERE cancel_outbox_id='cancel_phase'`).Scan(&got); err != nil || got != "awaiting_workspace_quiescence" {
					t.Fatalf("quiescent handoff=%q; want retryable workspace handoff: %v", got, err)
				}
				if applied, err := s.RecordAttemptCancelFailure(ctx, RecordAttemptCancelFailureRequest{
					CancelOutboxID: "cancel_phase", ExpectedHandoffState: phase,
					HandoffState: "awaiting_quiescence", LastError: "stop request returned late", UpdatedAt: "2026-03-01T12:00:05Z",
				}); err != nil || applied {
					t.Fatalf("late stop write replaced durable proof promotion: applied=%t err=%v", applied, err)
				}
			})
		}
	}
}

func TestStoreListPendingProviderProcessGuardsUsesKeyset(t *testing.T) {
	t.Parallel()
	s, _, req := providerProcessGuardFixture(t)
	firstAck := providerProcessGuardAcknowledgement(req, "guard_a", 1)
	secondAck := providerProcessGuardAcknowledgement(req, "guard_b", 2)
	if _, err := s.AcknowledgeProviderProcessGuard(context.Background(), firstAck); err != nil {
		t.Fatalf("acknowledge first guard: %v", err)
	}
	if _, err := s.AcknowledgeProviderProcessGuard(context.Background(), secondAck); err != nil {
		t.Fatalf("acknowledge second guard: %v", err)
	}
	if _, err := s.IngestProviderProcessGuardQuiescence(context.Background(), providerProcessGuardQuiescenceProof(firstAck)); err != nil {
		t.Fatalf("quiesce first guard: %v", err)
	}
	pending, err := s.ListPendingProviderProcessGuards(context.Background(), "", 1)
	if err != nil || len(pending) != 1 || pending[0].GuardID != firstAck.GuardID {
		t.Fatalf("unexpected pending guard page: guards=%#v err=%v", pending, err)
	}
	last, err := s.ListPendingProviderProcessGuards(context.Background(), pending[0].GuardID, 1)
	if err != nil || len(last) != 1 || last[0].GuardID != secondAck.GuardID {
		t.Fatalf("unexpected pending guard continuation: guards=%#v err=%v", last, err)
	}
	if _, err := s.ListPendingProviderProcessGuards(context.Background(), "", 0); err == nil {
		t.Fatal("expected positive pending guard limit")
	}
}

func TestStoreQuarantinesMissingGuardianEvidenceWithoutProvingQuiescence(t *testing.T) {
	t.Parallel()

	s, db, req := providerProcessGuardFixture(t)
	ack := providerProcessGuardAcknowledgement(req, "guard_evidence_lost", 1)
	if _, err := s.AcknowledgeProviderProcessGuard(context.Background(), ack); err != nil {
		t.Fatalf("acknowledge guard: %v", err)
	}
	quarantined, err := s.QuarantineProviderProcessGuardEvidence(context.Background(), ack.GuardID, "2026-03-01T12:00:04Z")
	if err != nil || !quarantined {
		t.Fatalf("quarantine missing guardian evidence: quarantined=%t err=%v", quarantined, err)
	}

	var lifecycle string
	var quarantinedAt sql.NullString
	if err := db.QueryRow(`SELECT lifecycle, quiescence_evidence_lost_at FROM provider_process_guards WHERE guard_id = ?`, ack.GuardID).Scan(&lifecycle, &quarantinedAt); err != nil {
		t.Fatalf("load quarantined guard: %v", err)
	}
	if lifecycle != string(ProviderProcessGuardLifecycleAcknowledged) || !quarantinedAt.Valid {
		t.Fatalf("evidence loss must remain non-quiescent: lifecycle=%q quarantined_at=%#v", lifecycle, quarantinedAt)
	}
	quiescent, err := s.AttemptProcessGuardsQuiescent(context.Background(), ack.AttemptID)
	if err != nil || quiescent {
		t.Fatalf("evidence-loss quarantine authorized quiescence: quiescent=%t err=%v", quiescent, err)
	}
	recoverable, err := s.AttemptProcessGuardEvidenceQuarantined(context.Background(), ack.AttemptID)
	if err != nil || !recoverable {
		t.Fatalf("expected durable evidence-loss quarantine: recoverable=%t err=%v", recoverable, err)
	}
	if err := s.QueueAttemptCancel(context.Background(), QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_evidence_lost", AttemptID: ack.AttemptID, RunID: ack.RunID, NodeRunID: ack.NodeRunID, ReasonCode: "agent_unresponsive", RequestedAt: "2026-03-01T12:00:04Z", CreatedAt: "2026-03-01T12:00:04Z",
	}); err != nil {
		t.Fatalf("queue cancellation handoff: %v", err)
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET state='committed_cancelled', lease_owner='', lease_expires_at=NULL WHERE attempt_id=?`, ack.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.QuarantineProviderProcessGuardEvidence(context.Background(), ack.GuardID, "2026-03-01T12:00:04Z"); err != nil {
		t.Fatalf("quarantine cancellation handoff: %v", err)
	}
	if _, err := s.IngestProviderProcessGuardQuiescence(context.Background(), providerProcessGuardQuiescenceProof(ack)); err != nil {
		t.Fatalf("ingest delayed valid guardian evidence: %v", err)
	}
	var handoffState string
	if err := db.QueryRow(`SELECT handoff_state FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel_evidence_lost'`).Scan(&handoffState); err != nil || handoffState != "awaiting_workspace_quiescence" {
		t.Fatalf("valid guardian evidence did not resume cancellation handoff: state=%q err=%v", handoffState, err)
	}
}

func TestAttemptProcessGuardsQuiescentRejectsAcknowledgedPrelaunchGuard(t *testing.T) {
	t.Parallel()

	s, db, req := providerProcessGuardFixture(t)
	if _, err := db.Exec(`UPDATE attempt_executions
		SET state = 'starting', launch_started_at = NULL, started_at = NULL, provider_session_id = ''
		WHERE attempt_id = ?`, req.Settlement.AttemptID); err != nil {
		t.Fatalf("prepare prelaunch attempt: %v", err)
	}
	ack := providerProcessGuardAcknowledgement(req, "guard_prelaunch_acknowledged", 1)
	if _, err := s.AcknowledgeProviderProcessGuard(context.Background(), ack); err != nil {
		t.Fatalf("acknowledge prelaunch provider guard: %v", err)
	}

	quiescent, err := s.AttemptProcessGuardsQuiescent(context.Background(), req.Settlement.AttemptID)
	if err != nil {
		t.Fatalf("check prelaunch provider guard quiescence: %v", err)
	}
	if quiescent {
		t.Fatal("acknowledged launch guard must block quiescence before launch observation is recorded")
	}
}

func providerProcessGuardFixture(t *testing.T) (*Store, *sql.DB, SettleProviderCompletionAndTerminalRequest) {
	t.Helper()
	return providerProcessGuardFixtureWithDB(t, openTestDB(t))
}

func providerProcessGuardFixtureWithDB(t *testing.T, db *sql.DB) (*Store, *sql.DB, SettleProviderCompletionAndTerminalRequest) {
	t.Helper()
	s, db, req := atomicProviderSettlementFixtureWithDB(t, db)
	mustExec(t, db, `DELETE FROM provider_process_guards`)
	return s, db, req
}

func providerProcessGuardAcknowledgement(req SettleProviderCompletionAndTerminalRequest, guardID string, commandOrdinal int64) AcknowledgeProviderProcessGuardRequest {
	return AcknowledgeProviderProcessGuardRequest{
		GuardID:               guardID,
		ProviderInvocationID:  req.Settlement.ProviderInvocationID,
		AttemptID:             req.Settlement.AttemptID,
		RunID:                 req.Settlement.RunID,
		NodeRunID:             req.Settlement.NodeRunID,
		CommandOrdinal:        commandOrdinal,
		Platform:              "unix_process_group",
		GuardianPID:           2001 + commandOrdinal,
		GuardianIdentityToken: fmt.Sprintf("guardian-%d", commandOrdinal),
		ChildPID:              3001 + commandOrdinal,
		ChildIdentityToken:    fmt.Sprintf("child-%d", commandOrdinal),
		TreeID:                fmt.Sprintf("%d", 3001+commandOrdinal),
		TreeIdentityToken:     fmt.Sprintf("tree-%d", commandOrdinal),
		QuiescenceCapability:  "capability-" + guardID,
		ExpectedLeaseOwner:    req.ExpectedLeaseOwner,
		ExpectedLeaseVersion:  req.ExpectedLeaseVersion,
		StartedAt:             "2026-03-01T12:00:02Z",
		AcknowledgedAt:        "2026-03-01T12:00:03Z",
	}
}

func providerProcessGuardQuiescenceProof(ack AcknowledgeProviderProcessGuardRequest) IngestProviderProcessGuardQuiescenceRequest {
	proof := IngestProviderProcessGuardQuiescenceRequest{
		GuardID:               ack.GuardID,
		ProviderInvocationID:  ack.ProviderInvocationID,
		AttemptID:             ack.AttemptID,
		RunID:                 ack.RunID,
		NodeRunID:             ack.NodeRunID,
		Platform:              ack.Platform,
		GuardianPID:           ack.GuardianPID,
		GuardianIdentityToken: ack.GuardianIdentityToken,
		ChildPID:              ack.ChildPID,
		ChildIdentityToken:    ack.ChildIdentityToken,
		TreeID:                ack.TreeID,
		TreeIdentityToken:     ack.TreeIdentityToken,
		QuiescenceCapability:  ack.QuiescenceCapability,
		ExitedAt:              "2026-03-01T12:00:04Z",
		QuiescentAt:           "2026-03-01T12:00:04Z",
		EvidenceJSON:          "",
	}
	proof.EvidenceJSON = providerProcessGuardEvidenceJSON(proof, false)
	proof.EvidenceSHA256 = sha256Hex(proof.EvidenceJSON)
	return proof
}

func providerProcessGuardEvidenceJSON(proof IngestProviderProcessGuardQuiescenceRequest, forced bool) string {
	return fmt.Sprintf(`{"type":"result","result":{"GuardID":%q,"ChildPID":%d,"TreeID":%s,"Platform":%q,"Quiescent":true,"ExitCode":%d,"ExitSignal":%d,"Forced":%t,"FailureCode":%q,"FailureText":%q}}`, proof.GuardID, proof.ChildPID, proof.TreeID, proof.Platform, proof.ExitCode, proof.ExitSignal, forced, proof.FailureCode, proof.FailureText)
}
