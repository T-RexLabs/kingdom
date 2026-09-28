package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kingdom/internal/processguardian"
	"kingdom/internal/testutil"
)

func TestFinalizeOperationReplaysPendingGuardEvidenceBeforeTimeoutSettlement(t *testing.T) {
	ctx := context.Background()
	store, operation, settlement := preparePlanningFinalizerGuardReplay(t, "valid")
	now := time.Now().UTC().Format(time.RFC3339Nano)

	// The quiescence callback was intentionally omitted. The durable record is
	// the sole proof available to this terminal finalizer.
	err := store.FinalizeOperation(ctx, FinalizeOperationRequest{
		Kind:               FinalizeFail,
		SessionID:          operation.PlanningSessionID,
		OperationID:        operation.OperationID,
		LeaseOwner:         operation.LeaseOwner,
		LeaseVersion:       operation.LeaseVersion,
		ConversationState:  PlanningSessionConversationStateReady,
		UpdatedAt:          now,
		FinishedAt:         now,
		ErrorMessage:       "planner timed out",
		ProviderSettlement: settlement,
	})
	if err != nil {
		t.Fatalf("finalize operation with durable guard evidence: %v", err)
	}

	persisted, found, err := store.GetOperation(ctx, operation.OperationID)
	if err != nil || !found {
		t.Fatalf("get finalized operation: found=%t err=%v", found, err)
	}
	if persisted.Status != PlanningOperationStatusFailed || persisted.CanonicalSettlementID == nil {
		t.Fatalf("expected canonical failed settlement, got %#v", persisted)
	}
	completed, found, err := store.GetPlanningProviderCompletionSettlement(ctx, *persisted.CanonicalSettlementID)
	if err != nil || !found {
		t.Fatalf("get canonical timeout settlement: found=%t err=%v", found, err)
	}
	if completed.Outcome != PlanningProviderCompletionOutcomeFailed || completed.FailureReasonCode != "timeout" {
		t.Fatalf("expected failed timeout settlement, got %#v", completed)
	}
	var lifecycle string
	if err := store.db.QueryRow(`SELECT lifecycle FROM provider_process_guards WHERE guard_id = 'guard_finalizer_replay'`).Scan(&lifecycle); err != nil {
		t.Fatalf("load replayed guard: %v", err)
	}
	if lifecycle != "quiescent" {
		t.Fatalf("expected durable guard evidence to be ingested, got lifecycle %q", lifecycle)
	}
}

func TestFinalizeOperationGuardEvidenceReplayFailsClosed(t *testing.T) {
	for _, evidence := range []string{"missing", "invalid"} {
		t.Run(evidence, func(t *testing.T) {
			ctx := context.Background()
			store, operation, settlement := preparePlanningFinalizerGuardReplay(t, evidence)
			now := time.Now().UTC().Format(time.RFC3339Nano)

			err := store.FinalizeOperation(ctx, FinalizeOperationRequest{
				Kind:               FinalizeFail,
				SessionID:          operation.PlanningSessionID,
				OperationID:        operation.OperationID,
				LeaseOwner:         operation.LeaseOwner,
				LeaseVersion:       operation.LeaseVersion,
				ConversationState:  PlanningSessionConversationStateReady,
				UpdatedAt:          now,
				FinishedAt:         now,
				ErrorMessage:       "planner timed out",
				ProviderSettlement: settlement,
			})
			var settlementErr *planningProviderSettlementError
			if !errors.As(err, &settlementErr) || settlementErr.Reason != "provider_process_guard_not_quiescent" {
				t.Fatalf("expected fail-closed guard rejection, got %v", err)
			}

			persisted, found, getErr := store.GetOperation(ctx, operation.OperationID)
			if getErr != nil || !found {
				t.Fatalf("get unfinalized operation: found=%t err=%v", found, getErr)
			}
			if persisted.Status != PlanningOperationStatusRunning || persisted.CanonicalSettlementID != nil {
				t.Fatalf("expected operation to remain live without settlement, got %#v", persisted)
			}
		})
	}
}

func TestPlanningProcessGuardQuiescenceCallbackIngestsDurableProof(t *testing.T) {
	ctx := context.Background()
	store, operation, _ := preparePlanningFinalizerGuardReplay(t, "valid")
	result, digest, evidence, err := processguardian.ReadQuiescenceEvidence(store.processGuardRuntimeRoot, "guard_finalizer_replay")
	if err != nil {
		t.Fatalf("read durable guard evidence: %v", err)
	}

	// This is the same capability-bound request built by the provider callback
	// when Start returns an aborted LaunchError.
	err = store.IngestPlanningProcessGuardQuiescence(ctx, PlanningProcessGuardQuiescence{
		GuardID: result.GuardID, Capability: result.QuiescenceCapability, ExitedAt: time.Now().UTC().Format(time.RFC3339Nano), QuiescentAt: time.Now().UTC().Format(time.RFC3339Nano),
		Platform: result.Platform, GuardianPID: result.GuardianPID, GuardianIdentityToken: result.GuardianIdentityToken, ChildPID: result.ChildPID, ChildIdentityToken: result.ChildIdentityToken,
		TreeID: fmt.Sprintf("%d", result.TreeID), TreeIdentityToken: result.TreeIdentityToken, ExitCode: result.ExitCode, ExitSignal: result.ExitSignal, Forced: result.Forced,
		FailureCode: result.FailureCode, FailureText: result.FailureText, EvidenceSHA256: digest, EvidenceJSON: evidence,
	})
	if err != nil {
		t.Fatalf("ingest callback quiescence: %v", err)
	}
	var lifecycle string
	if err := store.db.QueryRow(`SELECT lifecycle FROM provider_process_guards WHERE guard_id = 'guard_finalizer_replay'`).Scan(&lifecycle); err != nil {
		t.Fatalf("load ingested guard: %v", err)
	}
	if lifecycle != "quiescent" {
		t.Fatalf("durable callback left guard acknowledged: lifecycle=%q operation=%s", lifecycle, operation.OperationID)
	}
	unresolved, err := store.PlanningOperationHasUnresolvedProcessGuard(ctx, operation.OperationID)
	if err != nil || unresolved {
		t.Fatalf("expected no stranded acknowledged guard: unresolved=%t err=%v", unresolved, err)
	}
}

func TestAcknowledgePlanningProcessGuardConfirmsDurableCommitErrorAndUnblocksFinalization(t *testing.T) {
	store, _, connector, operation, ownership, now := newPlanningProcessGuardCommitFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connector.FailNextDurableCommit(cancel)
	if err := store.AcknowledgePlanningProcessGuard(ctx, ownership); err != nil {
		t.Fatalf("confirm planning guard after durable commit error: %v", err)
	}
	if ctx.Err() != context.Canceled {
		t.Fatal("expected the commit fault to cancel the request before readback")
	}
	connector.FailNextDurableCommit(nil)
	if err := store.AcknowledgePlanningProcessGuard(context.Background(), ownership); err != nil {
		t.Fatalf("confirm exact planning guard replay after durable commit error: %v", err)
	}

	finalize := planningGuardFailureFinalization(operation, now.Add(2*time.Second))
	err := store.FinalizeOperation(context.Background(), finalize)
	var settlementErr *planningProviderSettlementError
	if !errors.As(err, &settlementErr) || settlementErr.Reason != "provider_process_guard_not_quiescent" {
		t.Fatalf("expected an acknowledged guard to hold finalization until quiescence, got %v", err)
	}

	quiescentAt := now.Add(time.Second).Format(time.RFC3339Nano)
	result := processguardian.Result{
		Ownership: processguardian.Ownership{
			GuardID: ownership.GuardID, Platform: ownership.Platform,
			GuardianPID: ownership.GuardianPID, GuardianIdentityToken: ownership.GuardianIdentityToken,
			ChildPID: ownership.ChildPID, ChildIdentityToken: ownership.ChildIdentityToken,
			TreeID: 102, TreeIdentityToken: ownership.TreeIdentityToken,
			QuiescenceCapability: ownership.Capability,
		},
		ExitCode: 1, Forced: true, Quiescent: true,
		FailureCode: "provider_cancelled", FailureText: "guardian launch was cancelled",
	}
	evidence, err := json.Marshal(struct {
		Type   string                 `json:"type"`
		Result processguardian.Result `json:"result"`
	}{Type: "result", Result: result})
	if err != nil {
		t.Fatalf("marshal durable abort evidence: %v", err)
	}
	digest := sha256.Sum256(evidence)
	if err := store.IngestPlanningProcessGuardQuiescence(context.Background(), PlanningProcessGuardQuiescence{
		GuardID: ownership.GuardID, Capability: ownership.Capability,
		ExitedAt: quiescentAt, QuiescentAt: quiescentAt,
		Platform: ownership.Platform, GuardianPID: ownership.GuardianPID, GuardianIdentityToken: ownership.GuardianIdentityToken,
		ChildPID: ownership.ChildPID, ChildIdentityToken: ownership.ChildIdentityToken,
		TreeID: ownership.TreeID, TreeIdentityToken: ownership.TreeIdentityToken,
		ExitCode: result.ExitCode, ExitSignal: result.ExitSignal, Forced: result.Forced,
		FailureCode: result.FailureCode, FailureText: result.FailureText,
		EvidenceSHA256: fmt.Sprintf("%x", digest[:]), EvidenceJSON: string(evidence),
	}); err != nil {
		t.Fatalf("ingest durable abort evidence: %v", err)
	}
	if err := store.FinalizeOperation(context.Background(), finalize); err != nil {
		t.Fatalf("finalize planner operation after abort quiescence: %v", err)
	}
	persisted, found, err := store.GetOperation(context.Background(), operation.OperationID)
	if err != nil || !found || persisted.Status != PlanningOperationStatusFailed {
		t.Fatalf("planner operation remained blocked after durable abort evidence: found=%t status=%q err=%v", found, persisted.Status, err)
	}
}

func TestAcknowledgePlanningProcessGuardRejectsMismatchedDurableCommitReadback(t *testing.T) {
	store, db, connector, _, ownership, _ := newPlanningProcessGuardCommitFixture(t)
	if _, err := db.Exec(`CREATE TRIGGER alter_planning_guard_lease AFTER INSERT ON provider_process_guards WHEN NEW.guard_id = 'guard_planning_commit_then_error' BEGIN UPDATE provider_process_guards SET lease_version = lease_version + 1 WHERE guard_id = NEW.guard_id; END`); err != nil {
		t.Fatalf("create acknowledgement lineage trigger: %v", err)
	}
	connector.FailNextDurableCommit(nil)

	err := store.AcknowledgePlanningProcessGuard(context.Background(), ownership)
	if err == nil || !errors.Is(err, testutil.ErrDurableCommit) {
		t.Fatalf("expected the mismatched durable row to fail closed with the commit error, got %v", err)
	}
}

func newPlanningProcessGuardCommitFixture(t *testing.T) (*PlanningStore, *sql.DB, *testutil.SQLiteCommitFaultConnector, PlanningOperation, PlanningProcessGuardOwnership, time.Time) {
	t.Helper()
	connector := testutil.NewSQLiteCommitFaultConnector(":memory:")
	db := connector.OpenDB()
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	store, operation := newPlanningProviderSettlementTestStoreWithDB(t, db)
	now := time.Now().UTC()
	if _, err := db.Exec(`UPDATE planning_operations SET lease_expires_at = ? WHERE operation_id = ?`, now.Add(5*time.Minute).Format(time.RFC3339Nano), operation.OperationID); err != nil {
		t.Fatalf("extend planning operation lease: %v", err)
	}
	processing := PlanningSessionConversationStateProcessing
	if err := store.UpdateSession(context.Background(), UpdatePlanningSessionRequest{
		PlanningSessionID:  operation.PlanningSessionID,
		ConversationState:  &processing,
		CurrentOperationID: &operation.OperationID,
		UpdatedAt:          now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("mark planning session processing: %v", err)
	}
	if err := store.RequirePlanningProviderProcessGuard(context.Background(), "ppinv_test_1", operation.OperationID); err != nil {
		t.Fatalf("require planning process guard: %v", err)
	}
	startedAt := now.Add(-time.Second).Format(time.RFC3339Nano)
	ownership := PlanningProcessGuardOwnership{
		GuardID: "guard_planning_commit_then_error", PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID: operation.OperationID, PlanningSessionID: operation.PlanningSessionID,
		CommandOrdinal: 1, Platform: "unix_process_group",
		GuardianPID: 101, GuardianIdentityToken: "guardian-identity",
		ChildPID: 102, ChildIdentityToken: "child-identity", TreeID: "102", TreeIdentityToken: "tree-identity",
		Capability: "capability_planning_commit_then_error", LeaseOwner: operation.LeaseOwner, LeaseVersion: operation.LeaseVersion,
		StartedAt: startedAt, AcknowledgedAt: now.Format(time.RFC3339Nano),
	}
	return store, db, connector, operation, ownership, now
}

func planningGuardFailureFinalization(operation PlanningOperation, settledAt time.Time) FinalizeOperationRequest {
	stamp := settledAt.UTC().Format(time.RFC3339Nano)
	return FinalizeOperationRequest{
		Kind: FinalizeFail, SessionID: operation.PlanningSessionID, OperationID: operation.OperationID,
		LeaseOwner: operation.LeaseOwner, LeaseVersion: operation.LeaseVersion,
		ConversationState: PlanningSessionConversationStateReady,
		UpdatedAt:         stamp, FinishedAt: stamp, ErrorMessage: "planner launch failed",
		ProviderSettlement: &FinalizePlanningProviderSettlementRequest{
			SettlementID:                 "settlement_planning_guard_commit_then_error",
			PlanningProviderInvocationID: "ppinv_test_1",
			PrimaryEvidenceID:            "evidence_planning_guard_commit_then_error",
			PrimaryEvidenceKind:          PlanningProviderCompletionEvidenceKindStreamObservation,
			PrimaryEvidencePayloadJSON:   `{"state":"failed"}`,
			PrimaryEvidenceCreatedAt:     stamp,
			Outcome:                      PlanningProviderCompletionOutcomeFailed,
			FailureReasonCode:            "internal_error", FailureReasonText: "planner launch failed",
			SettledAt: stamp, RequiresProcessGuard: true,
		},
	}
}

func TestPlanningProcessGuardReplayCompletesCleanupWhenQuiescentEvidenceIsMissing(t *testing.T) {
	ctx := context.Background()
	store, _, _ := preparePlanningFinalizerGuardReplay(t, "valid")
	guardID, _, evidencePath := persistPlanningGuardReplayProof(t, store)
	if err := os.Remove(evidencePath); err != nil {
		t.Fatalf("remove already-ingested guardian evidence: %v", err)
	}

	pending, err := store.PlanningProcessGuardReplayPending(ctx, "")
	if err != nil || !pending {
		t.Fatalf("expected quiescent guard cleanup to be pending before replay: pending=%t err=%v", pending, err)
	}
	pass, err := store.ReplayPlanningProcessGuardEvidencePass(ctx, store.processGuardRuntimeRoot, "", 1)
	if err != nil || pass.Visited != 1 || pass.NextCursor != guardID {
		t.Fatalf("replay missing quiescent evidence: pass=%#v err=%v", pass, err)
	}
	var lifecycle string
	var cleanupPending int
	if err := store.db.QueryRow(`SELECT lifecycle, evidence_cleaned_at IS NULL FROM provider_process_guards WHERE guard_id = ?`, guardID).Scan(&lifecycle, &cleanupPending); err != nil {
		t.Fatalf("load replayed planning guard: %v", err)
	}
	if lifecycle != "quiescent" || cleanupPending != 0 {
		t.Fatalf("missing evidence did not complete cleanup from durable proof: lifecycle=%q cleanup_pending=%d", lifecycle, cleanupPending)
	}
	pending, err = store.PlanningProcessGuardReplayPending(ctx, "")
	if err != nil || pending {
		t.Fatalf("planning guard remained pending after replay: pending=%t err=%v", pending, err)
	}
}

func TestPlanningProcessGuardReplayDoesNotCompleteCleanupWithoutTrustedProof(t *testing.T) {
	for _, tc := range []struct {
		name        string
		evidence    string
		quiescent   bool
		replacement bool
	}{
		{name: "acknowledged missing", evidence: "missing"},
		{name: "quiescent corrupt", evidence: "valid", quiescent: true},
		{name: "quiescent replaced", evidence: "valid", quiescent: true, replacement: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, _, _ := preparePlanningFinalizerGuardReplay(t, tc.evidence)
			guardID := "guard_finalizer_replay"
			if tc.quiescent {
				_, _, evidencePath := persistPlanningGuardReplayProof(t, store)
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
					t.Fatalf("replace planning guard evidence: %v", err)
				}
			}

			if _, err := store.ReplayPlanningProcessGuardEvidencePass(ctx, store.processGuardRuntimeRoot, "", 1); err != nil {
				t.Fatalf("replay untrusted planning guard evidence: %v", err)
			}
			var lifecycle string
			var cleanupPending int
			if err := store.db.QueryRow(`SELECT lifecycle, evidence_cleaned_at IS NULL FROM provider_process_guards WHERE guard_id = ?`, guardID).Scan(&lifecycle, &cleanupPending); err != nil {
				t.Fatalf("load planning guard after untrusted replay: %v", err)
			}
			wantLifecycle := "acknowledged"
			if tc.quiescent {
				wantLifecycle = "quiescent"
			}
			if lifecycle != wantLifecycle || cleanupPending != 1 {
				t.Fatalf("untrusted evidence advanced guard cleanup: lifecycle=%q want=%q cleanup_pending=%d", lifecycle, wantLifecycle, cleanupPending)
			}
			pending, err := store.PlanningProcessGuardReplayPending(ctx, "")
			if err != nil || !pending {
				t.Fatalf("untrusted evidence stopped replay retry: pending=%t err=%v", pending, err)
			}
		})
	}
}

func TestPlanningProcessGuardCleanupAcceptsConcurrentEvidenceRemoval(t *testing.T) {
	ctx := context.Background()
	store, _, _ := preparePlanningFinalizerGuardReplay(t, "valid")
	guardID, evidence, evidencePath := persistPlanningGuardReplayProof(t, store)
	if err := os.Remove(evidencePath); err != nil {
		t.Fatalf("remove evidence after replay read: %v", err)
	}

	store.removeAndMarkPlanningProcessGuardEvidenceCleaned(ctx, store.processGuardRuntimeRoot, guardID, evidence)
	pending, err := store.PlanningProcessGuardReplayPending(ctx, "")
	if err != nil || pending {
		t.Fatalf("concurrent evidence removal did not complete cleanup: pending=%t err=%v", pending, err)
	}
}

func TestPlanningProcessGuardCleanupMarkerFailureIsRetryable(t *testing.T) {
	ctx := context.Background()
	store, _, _ := preparePlanningFinalizerGuardReplay(t, "valid")
	guardID, _, evidencePath := persistPlanningGuardReplayProof(t, store)
	if err := os.Remove(evidencePath); err != nil {
		t.Fatalf("remove already-ingested guardian evidence: %v", err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER fail_planning_guard_cleanup_marker BEFORE UPDATE OF evidence_cleaned_at ON provider_process_guards WHEN NEW.guard_id = 'guard_finalizer_replay' BEGIN SELECT RAISE(ABORT, 'cleanup marker unavailable'); END`); err != nil {
		t.Fatalf("create cleanup marker failure trigger: %v", err)
	}
	if _, err := store.ReplayPlanningProcessGuardEvidencePass(ctx, store.processGuardRuntimeRoot, "", 1); err != nil {
		t.Fatalf("cleanup marker failure turned durable quiescence into replay failure: %v", err)
	}
	pending, err := store.PlanningProcessGuardReplayPending(ctx, "")
	if err != nil || !pending {
		t.Fatalf("failed cleanup marker was not left retryable: pending=%t err=%v", pending, err)
	}
	if _, err := store.db.Exec(`DROP TRIGGER fail_planning_guard_cleanup_marker`); err != nil {
		t.Fatalf("remove cleanup marker failure trigger: %v", err)
	}
	if _, err := store.ReplayPlanningProcessGuardEvidencePass(ctx, store.processGuardRuntimeRoot, "", 1); err != nil {
		t.Fatalf("retry planning guard cleanup marker: %v", err)
	}
	pending, err = store.PlanningProcessGuardReplayPending(ctx, "")
	if err != nil || pending {
		t.Fatalf("planning guard cleanup did not converge after retry: pending=%t err=%v", pending, err)
	}
	var lifecycle string
	var cleanupPending int
	if err := store.db.QueryRow(`SELECT lifecycle, evidence_cleaned_at IS NULL FROM provider_process_guards WHERE guard_id = ?`, guardID).Scan(&lifecycle, &cleanupPending); err != nil {
		t.Fatalf("load retried planning guard: %v", err)
	}
	if lifecycle != "quiescent" || cleanupPending != 0 {
		t.Fatalf("retry changed valid quiescence or left cleanup pending: lifecycle=%q cleanup_pending=%d", lifecycle, cleanupPending)
	}
}

func persistPlanningGuardReplayProof(t *testing.T, store *PlanningStore) (string, string, string) {
	t.Helper()
	const guardID = "guard_finalizer_replay"
	result, digest, evidence, err := processguardian.ReadQuiescenceEvidence(store.processGuardRuntimeRoot, guardID)
	if err != nil {
		t.Fatalf("read planning guard evidence before ingest: %v", err)
	}
	quiescentAt := time.Now().UTC().Format(time.RFC3339Nano)
	if err := store.IngestPlanningProcessGuardQuiescence(context.Background(), PlanningProcessGuardQuiescence{
		GuardID: guardID, Capability: result.QuiescenceCapability, ExitedAt: quiescentAt, QuiescentAt: quiescentAt,
		Platform: result.Platform, GuardianPID: result.GuardianPID, GuardianIdentityToken: result.GuardianIdentityToken,
		ChildPID: result.ChildPID, ChildIdentityToken: result.ChildIdentityToken, TreeID: fmt.Sprintf("%d", result.TreeID),
		TreeIdentityToken: result.TreeIdentityToken, ExitCode: result.ExitCode, ExitSignal: result.ExitSignal, Forced: result.Forced,
		FailureCode: result.FailureCode, FailureText: result.FailureText, EvidenceSHA256: digest, EvidenceJSON: evidence,
	}); err != nil {
		t.Fatalf("ingest planning guard evidence: %v", err)
	}
	name := sha256.Sum256([]byte(guardID))
	evidencePath := filepath.Join(store.processGuardRuntimeRoot, "provider-process-guards", "quiescence", fmt.Sprintf("%x.json", name[:]))
	return guardID, evidence, evidencePath
}

func preparePlanningFinalizerGuardReplay(t *testing.T, evidenceKind string) (*PlanningStore, PlanningOperation, *FinalizePlanningProviderSettlementRequest) {
	t.Helper()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)
	runtimeRoot := t.TempDir()
	store.SetProcessGuardRuntimeRoot(runtimeRoot)
	now := time.Now().UTC()
	leaseExpiresAt := now.Add(time.Minute).Format(time.RFC3339Nano)
	if _, err := store.db.Exec(`UPDATE planning_operations SET lease_expires_at = ? WHERE operation_id = ?`, leaseExpiresAt, operation.OperationID); err != nil {
		t.Fatalf("extend planning operation lease: %v", err)
	}
	processing := PlanningSessionConversationStateProcessing
	if err := store.UpdateSession(ctx, UpdatePlanningSessionRequest{
		PlanningSessionID:  operation.PlanningSessionID,
		ConversationState:  &processing,
		CurrentOperationID: &operation.OperationID,
		UpdatedAt:          now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("mark planning session processing: %v", err)
	}
	if err := store.RequirePlanningProviderProcessGuard(ctx, "ppinv_test_1", operation.OperationID); err != nil {
		t.Fatalf("require planning process guard: %v", err)
	}

	capability := "capability_finalizer_replay"
	guard := processguardian.Result{Ownership: processguardian.Ownership{
		GuardID:               "guard_finalizer_replay",
		Platform:              "unix_process_group",
		GuardianPID:           101,
		GuardianIdentityToken: "guardian-identity",
		ChildPID:              102,
		ChildIdentityToken:    "child-identity",
		TreeID:                102,
		TreeIdentityToken:     "tree-identity",
		QuiescenceCapability:  capability,
	}, ExitCode: 124, Quiescent: true, FailureCode: "timeout", FailureText: "planner timed out"}
	acknowledgedAt := now.Format(time.RFC3339Nano)
	if err := store.AcknowledgePlanningProcessGuard(ctx, PlanningProcessGuardOwnership{
		GuardID: guard.GuardID, PlanningProviderInvocationID: "ppinv_test_1", PlanningOperationID: operation.OperationID, PlanningSessionID: operation.PlanningSessionID,
		CommandOrdinal: 1, Platform: guard.Platform, GuardianPID: guard.GuardianPID, GuardianIdentityToken: guard.GuardianIdentityToken,
		ChildPID: guard.ChildPID, ChildIdentityToken: guard.ChildIdentityToken, TreeID: fmt.Sprintf("%d", guard.TreeID), TreeIdentityToken: guard.TreeIdentityToken,
		Capability: capability, LeaseOwner: operation.LeaseOwner, LeaseVersion: operation.LeaseVersion, StartedAt: acknowledgedAt, AcknowledgedAt: acknowledgedAt,
	}); err != nil {
		t.Fatalf("acknowledge planning process guard: %v", err)
	}

	if evidenceKind != "missing" {
		path := filepath.Join(runtimeRoot, "provider-process-guards", "quiescence")
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatalf("create guard evidence directory: %v", err)
		}
		raw := []byte(`{"type":"invalid"}`)
		if evidenceKind == "valid" {
			var err error
			raw, err = json.Marshal(struct {
				Type   string                 `json:"type"`
				Result processguardian.Result `json:"result"`
			}{Type: "result", Result: guard})
			if err != nil {
				t.Fatalf("marshal guard evidence: %v", err)
			}
		}
		name := sha256.Sum256([]byte(guard.GuardID))
		if err := os.WriteFile(filepath.Join(path, fmt.Sprintf("%x.json", name)), raw, 0o600); err != nil {
			t.Fatalf("write guard evidence: %v", err)
		}
	}

	return store, operation, &FinalizePlanningProviderSettlementRequest{
		SettlementID:                 "settlement_guard_finalizer_replay",
		PlanningProviderInvocationID: "ppinv_test_1",
		PrimaryEvidenceID:            "evidence_guard_finalizer_replay",
		PrimaryEvidenceKind:          PlanningProviderCompletionEvidenceKindStreamObservation,
		PrimaryEvidencePayloadJSON:   `{"state":"failed"}`,
		PrimaryEvidenceCreatedAt:     now.Format(time.RFC3339Nano),
		Outcome:                      PlanningProviderCompletionOutcomeFailed,
		FailureReasonCode:            "timeout",
		FailureReasonText:            "planner timed out",
		SettledAt:                    now.Format(time.RFC3339Nano),
	}
}

func TestPlanningFinalizerGuardReplayScopesEvidenceToInvocation(t *testing.T) {
	ctx := context.Background()
	store, operation, settlement := preparePlanningFinalizerGuardReplay(t, "valid")
	if _, err := store.db.Exec(`UPDATE provider_process_guards SET provider_invocation_id = 'ppinv_other' WHERE guard_id = 'guard_finalizer_replay'`); err != nil {
		t.Fatalf("move guard to another invocation: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	err := store.FinalizeOperation(ctx, FinalizeOperationRequest{
		Kind: FinalizeFail, SessionID: operation.PlanningSessionID, OperationID: operation.OperationID,
		LeaseOwner: operation.LeaseOwner, LeaseVersion: operation.LeaseVersion, ConversationState: PlanningSessionConversationStateReady,
		UpdatedAt: now, FinishedAt: now, ErrorMessage: "planner timed out", ProviderSettlement: settlement,
	})
	if err != nil {
		t.Fatalf("unrelated invocation guard must not block finalization: %v", err)
	}
	var lifecycle string
	if err := store.db.QueryRow(`SELECT lifecycle FROM provider_process_guards WHERE guard_id = 'guard_finalizer_replay'`).Scan(&lifecycle); err != nil {
		t.Fatalf("load unrelated guard: %v", err)
	}
	if lifecycle != "acknowledged" {
		t.Fatalf("finalizer replay must not ingest another invocation's evidence, got lifecycle %q", lifecycle)
	}
}
