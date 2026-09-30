package api

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"kingdom/internal/idempotency"
	"kingdom/internal/state"
)

func TestMutationOutcomeReadsOnlyOwnedNonsecretResults(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	store := state.NewIdempotencyStore(db)
	key, err := idempotency.Generate("recovery", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := idempotency.Parse(key)
	if err != nil {
		t.Fatal(err)
	}
	record := state.IdempotencyRecord{ID: "recovery", Route: "POST /acp/runs", ActorID: operatorActorID, ClientKey: key, RequestHash: "original-binding-hash", CreatedAt: time.Now().UTC().Format(time.RFC3339), UpdatedAt: time.Now().UTC().Format(time.RFC3339)}
	read := func(operation string) map[string]any {
		t.Helper()
		res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/mutation-outcomes/" + key + "?operation=" + operation})
		if res.Code != http.StatusOK {
			t.Fatalf("outcome read: %d %s", res.Code, res.Body.String())
		}
		var value map[string]any
		if err := json.Unmarshal(res.Body.Bytes(), &value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	if got := read("planning-operation-create"); got["state"] != "unknown" {
		t.Fatalf("missing record: %#v", got)
	}
	if _, _, err := store.ReserveVersioned(t.Context(), record, parsed, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := read("planning-operation-create"); got["state"] != "pending" {
		t.Fatalf("pending record: %#v", got)
	}
	record.ResponseStatus = http.StatusCreated
	record.ResponseBody = `{"run_id":"operation-123"}`
	record.ResponseHeaders = `{"X-Private":["never expose"]}`
	if err := store.Finalize(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	got := read("planning-operation-create")
	if got["state"] != "completed" || got["response_status"] != float64(http.StatusCreated) || got["response_body"].(map[string]any)["run_id"] != "operation-123" {
		t.Fatalf("completed record: %#v", got)
	}
	if _, exposed := got["response_headers"]; exposed {
		t.Fatal("outcome exposes headers")
	}
	if got := read("run-create"); got["state"] != "unknown" {
		t.Fatalf("cross-operation result: %#v", got)
	}
	if _, err := db.Exec(`UPDATE idempotency_keys SET actor_id = 'another-actor' WHERE idempotency_key_id = 'recovery'`); err != nil {
		t.Fatal(err)
	}
	if got := read("planning-operation-create"); got["state"] != "unknown" {
		t.Fatalf("cross-actor result: %#v", got)
	}
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/mutation-outcomes/" + key + "?operation=github-setup"})
	if res.Code != http.StatusBadRequest {
		t.Fatalf("unsupported sensitive operation: %d %s", res.Code, res.Body.String())
	}
}
