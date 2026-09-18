package state

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kingdom/internal/processguardian"
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
