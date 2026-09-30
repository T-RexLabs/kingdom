package state

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"kingdom/internal/core/providerexecution"
)

func TestActivityContentAtomicPublicationPagingAndScope(t *testing.T) {
	store, operation := newPlanningProviderSettlementTestStore(t)
	body := strings.Repeat("exact 界😀\n", 18000) + "tail\n"
	req := AppendPlanningOperationEventRequest{EventID: "activity-content-1", OperationID: operation.OperationID, Seq: 1, EventType: "generic", PayloadJSON: `{"kind":"kingdom.tool","activity_id":"call-1","tool_name":"read","tool_status":"completed","occurred_at":"2026-09-30T10:00:00Z"}`, CreatedAt: "2026-09-30T10:00:03Z", ActivityContents: map[string]providerexecution.ActivityContent{"input": providerexecution.ActivityText(""), "output": providerexecution.ActivityText(body)}}
	event, err := store.AppendOperationEvent(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Sequence  int                               `json:"sequence"`
		Output    providerexecution.ActivityContent `json:"output"`
		Input     providerexecution.ActivityContent `json:"input"`
		Reasoning providerexecution.ActivityContent `json:"reasoning"`
	}
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Sequence != 1 || payload.Output.ContentURL == "" || payload.Output.ByteLength == nil || *payload.Output.ByteLength != int64(len(body)) || len(event.PayloadJSON) > 6000 {
		t.Fatalf("invalid bounded activity reference: %s", event.PayloadJSON)
	}
	if payload.Input.Availability != "available" || payload.Input.ByteLength == nil || *payload.Input.ByteLength != 0 || payload.Reasoning.Availability != "not_provided" {
		t.Fatal("empty and absent content conflated")
	}
	scope := ActivityScope{OperationID: operation.OperationID}
	var actual strings.Builder
	for offset := int64(0); ; {
		page, err := ReadActivityContentPage(t.Context(), store.db, scope, req.EventID, "output", offset, 1001)
		if err != nil {
			t.Fatal(err)
		}
		if !utf8.ValidString(page.Text) || len(page.Text) > 1001 {
			t.Fatal("page split UTF-8 or exceeded bound")
		}
		actual.WriteString(page.Text)
		if page.NextOffset == 0 {
			break
		}
		offset = page.NextOffset
	}
	if actual.String() != body {
		t.Fatal("full activity result did not survive paging exactly")
	}
	if _, err := ReadActivityContentPage(t.Context(), store.db, ActivityScope{OperationID: "wrong"}, req.EventID, "output", 0, 1000); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("cross-operation content read: %v", err)
	}
	empty, err := ReadActivityContentPage(t.Context(), store.db, scope, req.EventID, "input", 0, 1000)
	if err != nil || empty.Text != "" || empty.ByteLength != 0 {
		t.Fatalf("empty result read: %+v %v", empty, err)
	}
}

func TestActivityContentFailureDoesNotPublishAnEventOrPartialChunks(t *testing.T) {
	store, operation := newPlanningProviderSettlementTestStore(t)
	bad := providerexecution.ActivityText("actual")
	bad.SHA256 = strings.Repeat("0", 64)
	_, err := store.AppendOperationEvent(t.Context(), AppendPlanningOperationEventRequest{EventID: "activity-content-bad", OperationID: operation.OperationID, Seq: 1, EventType: "generic", PayloadJSON: `{"kind":"kingdom.tool","activity_id":"call-1","tool_name":"read"}`, CreatedAt: "2026-09-30T10:00:03Z", ActivityContents: map[string]providerexecution.ActivityContent{"output": bad}})
	if err == nil {
		t.Fatal("changed source content accepted")
	}
	for _, query := range []string{`SELECT count(*) FROM planning_operation_events WHERE event_id='activity-content-bad'`, `SELECT count(*) FROM request_contents WHERE content_id LIKE 'activity:%'`, `SELECT count(*) FROM request_content_chunks WHERE content_id LIKE 'activity:%'`} {
		var count int
		if err := store.db.QueryRow(query).Scan(&count); err != nil || count != 0 {
			t.Fatalf("partial content publication: count=%d err=%v", count, err)
		}
	}
}
