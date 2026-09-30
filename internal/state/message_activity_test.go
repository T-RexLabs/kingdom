package state

import (
	"encoding/json"
	"kingdom/internal/core/providerexecution"
	"strings"
	"testing"
)

func TestMessageActivityPersistsCompleteScopedContentAndHistory(t *testing.T) {
	store, op := newPlanningProviderSettlementTestStore(t)
	text := "\n" + strings.Repeat("message 界😀\n", 12000)
	event, err := store.AppendOperationEvent(t.Context(), AppendPlanningOperationEventRequest{EventID: "message-event", OperationID: op.OperationID, Seq: 1, EventType: "generic", PayloadJSON: `{"kind":"kingdom.message","activity_id":"message-part","invocation_id":"inv-message","provider_part_id":"message-part"}`, CreatedAt: "2026-09-30T10:00:00Z", ActivityContents: map[string]providerexecution.ActivityContent{"message": providerexecution.ActivityText(text)}})
	if err != nil {
		t.Fatal(err)
	}
	var p struct {
		Message providerexecution.ActivityContent `json:"message"`
	}
	if err := json.Unmarshal([]byte(event.PayloadJSON), &p); err != nil {
		t.Fatal(err)
	}
	if p.Message.ContentURL == "" {
		t.Fatal("message was not atomically published with a complete-content reference")
	}
	scope := ActivityScope{OperationID: op.OperationID}
	page, err := ListActivityEventPage(t.Context(), store.db, scope, nil, 2)
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("message absent from bounded history: %+v %v", page, err)
	}
	var actual strings.Builder
	for offset := int64(0); ; {
		part, err := ReadActivityContentPage(t.Context(), store.db, scope, event.EventID, "message", offset, 1001)
		if err != nil {
			t.Fatal(err)
		}
		actual.WriteString(part.Text)
		if part.NextOffset == 0 {
			break
		}
		offset = part.NextOffset
	}
	if actual.String() != text {
		t.Fatal("message bytes changed during paging")
	}
	if _, err := ReadActivityContentPage(t.Context(), store.db, ActivityScope{OperationID: "wrong"}, event.EventID, "message", 0, 1000); err == nil {
		t.Fatal("cross-operation message content exposed")
	}
}

func TestObservedRunMessageCannotSettleAttempt(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	req := baseAttemptEventIngestionRequest()
	req.AttemptID = "message-pending"
	req.EventID = "observed-message"
	req.EventType = AttemptActivityEventType
	req.Seq = 1
	req.PayloadJSON = `{"kind":"kingdom.message","activity_id":"message-part"}`
	req.ActivityContents = map[string]providerexecution.ActivityContent{"message": providerexecution.ActivityText("Finished successfully")}
	seedCanonicalAttemptExecutionRow(t, db, req.AttemptID, req.RunID, req.NodeRunID.String)
	var before, after string
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id=?`, req.AttemptID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAttemptEventIngestor(db, loadStateTestSchemas(t)).Ingest(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id=?`, req.AttemptID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("provider message acquired settlement authority: %s -> %s", before, after)
	}
}
