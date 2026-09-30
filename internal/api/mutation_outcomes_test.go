package api

import (
	"encoding/json"
	"net/http"
	"reflect"
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

func TestMutationOutcomeOrdinaryPlanningUpdatesAndEmptyDeletion(t *testing.T) {
	for _, tc := range []struct {
		operation, route, body string
		status                 int
	}{
		{"planning-session-update", "PATCH /planning-sessions/{id}", `{"title":"Saved title"}`, http.StatusOK},
		{"planning-session-delete", "DELETE /planning-sessions/{id}", "", http.StatusNoContent},
		{"planning-context-refresh", "POST /planning-sessions/{id}/context/refresh", `{"session":{"planning_session_id":"session"},"context_snapshot":{}}`, http.StatusOK},
		{"planning-message-activate", "PUT /planning-sessions/{id}/messages/{messageId}/activate", `{"planning_message_id":"selected-branch"}`, http.StatusOK},
		{"planning-source-attach", "POST /planning-sessions/{id}/external-sources", `{"source_id":"source"}`, http.StatusCreated},
		{"external-source-refresh", "POST /external-source-links/{id}/refreshes", `{"outcome":"unchanged","snapshot_created":false}`, http.StatusOK},
		{"external-source-adopt", "POST /external-source-links/{id}/adoptions", `{"status":"adopted"}`, http.StatusCreated},
		{"external-source-detach", "POST /external-source-links/{id}/detachments", `{"status":"detached"}`, http.StatusCreated},
	} {
		t.Run(tc.operation, func(t *testing.T) {
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
			now := time.Now().UTC().Format(time.RFC3339)
			record := state.IdempotencyRecord{ID: "ordinary-recovery", Route: tc.route, ActorID: operatorActorID, ClientKey: key, RequestHash: "original-binding", CreatedAt: now, UpdatedAt: now}
			if _, _, err := store.ReserveVersioned(t.Context(), record, parsed, time.Now()); err != nil {
				t.Fatal(err)
			}
			record.ResponseStatus, record.ResponseBody = tc.status, tc.body
			if err := store.Finalize(t.Context(), record); err != nil {
				t.Fatal(err)
			}
			result := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/mutation-outcomes/" + key + "?operation=" + tc.operation})
			if result.Code != http.StatusOK {
				t.Fatalf("read outcome: %d %s", result.Code, result.Body.String())
			}
			var value map[string]any
			if err := json.Unmarshal(result.Body.Bytes(), &value); err != nil {
				t.Fatal(err)
			}
			if value["state"] != "completed" || value["response_status"] != float64(tc.status) {
				t.Fatalf("wrong outcome: %#v", value)
			}
			if tc.status == http.StatusNoContent {
				if _, ok := value["response_body"]; ok {
					t.Fatalf("204 should omit response body: %#v", value)
				}
			} else {
				var expected map[string]any
				if err := json.Unmarshal([]byte(tc.body), &expected); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(value["response_body"], expected) {
					t.Fatalf("outcome changed original body: %#v", value)
				}
			}
			if _, err := db.Exec(`UPDATE idempotency_keys SET actor_id = 'another-actor' WHERE idempotency_key_id = 'ordinary-recovery'`); err != nil {
				t.Fatal(err)
			}
			other := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/mutation-outcomes/" + key + "?operation=" + tc.operation})
			if err := json.Unmarshal(other.Body.Bytes(), &value); err != nil {
				t.Fatal(err)
			}
			if value["state"] != "unknown" {
				t.Fatalf("cross-actor read leaked: %s", other.Body.String())
			}
		})
	}
}
