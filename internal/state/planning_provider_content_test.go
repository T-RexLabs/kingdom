package state

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kingdom/internal/core/providerexecution"
)

func TestPlanningProviderContentAtomicIntegrityAndReplay(t *testing.T) {
	store, operation := newPlanningProviderSettlementTestStore(t)
	ctx := context.Background()
	text := strings.Repeat("terminal 界 ", 30000) + "terminal-tail"
	stdout := strings.Repeat("diagnostic stdout ", 40000) + "stdout-tail"
	stderr := strings.Repeat("diagnostic stderr ", 20000) + "stderr-tail"
	request := streamObservationSuccessSettlementRequest(text, `{}`)
	observation, err := planningProviderTerminalStreamObservation(request.PrimaryEvidencePayloadJSON)
	if err != nil {
		t.Fatal(err)
	}
	for i, body := range []string{text, stdout, stderr} {
		path := filepath.Join(t.TempDir(), "content")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		ref := providerexecution.ReferenceText("ppinv_test_1", body)
		request.StagedContent = append(request.StagedContent, providerexecution.StagedContent{Reference: ref, Path: path})
		if i == 0 {
			observation.CanonicalTerminalTextRef = &ref
			observation.CanonicalTerminalText = ""
		} else {
			observation.DiagnosticContent = append(observation.DiagnosticContent, ref)
		}
	}
	raw, err := json.Marshal(map[string]any{"stream_observation": observation})
	if err != nil {
		t.Fatal(err)
	}
	request.PrimaryEvidencePayloadJSON = string(raw)
	bad := request.StagedContent[2]
	if err := os.WriteFile(bad.Path, []byte("corrupted"), 0600); err != nil {
		t.Fatal(err)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := finalizePlanningProviderSettlementTx(ctx, tx, operation.OperationID, operation.PlanningSessionID, request); err == nil {
		t.Fatal("corrupt diagnostic content was accepted")
	}
	tx.Rollback()
	for _, table := range []string{"planning_provider_contents", "planning_provider_content_chunks", "planning_provider_completion_settlements"} {
		var count int
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("partial publication %s count=%d err=%v", table, count, err)
		}
	}
	if err := os.WriteFile(bad.Path, []byte(stderr), 0600); err != nil {
		t.Fatal(err)
	}
	tx, err = store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := finalizePlanningProviderSettlementTx(ctx, tx, operation.OperationID, operation.PlanningSessionID, request)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for i, body := range []string{text, stdout, stderr} {
		staged := request.StagedContent[i]
		if err := os.Remove(staged.Path); err != nil {
			t.Fatal(err)
		}
		var out strings.Builder
		if err := store.CopyPlanningProviderContent(ctx, staged.Reference, &out); err != nil || out.String() != body {
			t.Fatalf("durable content replay %d failed: %v", i, err)
		}
	}
	settlement, found, err := store.GetPlanningProviderCompletionSettlement(ctx, id)
	if err != nil || !found || settlement.FinalText != text {
		t.Fatalf("canonical terminal text changed: %v", err)
	}
	wrong := request.StagedContent[0].Reference
	wrong.InvocationID = "other-invocation"
	if err := store.CopyPlanningProviderContent(ctx, wrong, &strings.Builder{}); err == nil {
		t.Fatal("cross-invocation content replay succeeded")
	}
}
