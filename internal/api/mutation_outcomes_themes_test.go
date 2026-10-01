package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/state"
	"kingdom/internal/theme"
)

func TestThemeMutationOutcomesAcrossBrowserBindings(t *testing.T) {
	store, err := theme.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ThemeStore: store})
	const origin = "http://localhost:61511"
	type browser struct {
		cookie  *http.Cookie
		session authSessionResponse
	}
	bootstrap := func() browser {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, origin+"/api/v1/auth/session", nil)
		req.RemoteAddr = "127.0.0.1:49152"
		req.Header.Set("Origin", origin)
		req.Header.Set(browserBootstrapHeader, "1")
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		if res.Code != http.StatusCreated || len(res.Result().Cookies()) != 1 {
			t.Fatalf("bootstrap: %d %s", res.Code, res.Body.String())
		}
		var session authSessionResponse
		if err := json.Unmarshal(res.Body.Bytes(), &session); err != nil {
			t.Fatal(err)
		}
		return browser{res.Result().Cookies()[0], session}
	}
	first, renewed := bootstrap(), bootstrap()
	if first.session.SessionID == renewed.session.SessionID {
		t.Fatal("test requires different browser bindings")
	}
	request := func(client browser, method, path, key string, body any) *httptest.ResponseRecorder {
		t.Helper()
		var raw []byte
		if body != nil {
			var err error
			raw, err = json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
		}
		req := httptest.NewRequest(method, origin+"/api/v1"+path, bytes.NewReader(raw))
		req.RemoteAddr = "127.0.0.1:49152"
		req.Header.Set("Content-Type", "application/json")
		if client.cookie != nil {
			req.AddCookie(client.cookie)
		}
		if method != http.MethodGet {
			req.Header.Set("Origin", origin)
			req.Header.Set("X-CSRF-Token", client.session.CSRFToken)
			req.Header.Set(idempotencyKeyHeader, key)
		}
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res
	}
	source := `{"schemaVersion":1,"resolverVersion":1,"id":"outcome-theme","version":"1.0.0","name":"Outcome Theme","base":{"id":"obsidian-court","version":"1.1.0"}}`
	resolved, err := store.Resolver().Resolve([]byte(source), nil)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := store.Resolver().ExportZIP(resolved)
	if err != nil {
		t.Fatal(err)
	}
	var sourceDigest string
	for _, tc := range []struct {
		operation, method, path, route string
		status                         int
		body                           func() any
	}{
		{"theme-source-save", "PUT", "/themes/outcome-theme/source", "PUT /themes/{id}/source", 200, func() any {
			return gen.ThemeSaveRequest{Source: source, Assets: map[string][]byte{}, ExpectedSourceDigest: ""}
		}},
		{"theme-publish", "POST", "/themes/outcome-theme/publish", "POST /themes/{id}/publish", 201, func() any { return gen.PublishThemeJSONBody{ExpectedSourceDigest: sourceDigest} }},
		{"theme-unpublish", "DELETE", "/themes/outcome-theme", "DELETE /themes/{id}", 204, func() any { return nil }},
		{"theme-source-delete", "DELETE", "/themes/outcome-theme/source", "DELETE /themes/{id}/source", 204, func() any { return gen.DeleteThemeSourceJSONBody{ExpectedSourceDigest: sourceDigest} }},
		{"theme-import", "POST", "/themes/import", "POST /themes/import", 201, func() any { return gen.ImportThemeJSONBody{Archive: archive} }},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			key := testIdempotencyKey(t, tc.operation, time.Now())
			body := tc.body()
			original := request(first, tc.method, tc.path, key, body)
			if original.Code != tc.status {
				t.Fatalf("mutation: %d %s", original.Code, original.Body.String())
			}
			if tc.operation == "theme-source-save" {
				var saved gen.ThemeSource
				if err := json.Unmarshal(original.Body.Bytes(), &saved); err != nil {
					t.Fatal(err)
				}
				sourceDigest = saved.SourceDigest
			}
			// The actual mux template must match the outcome lookup, not merely
			// a test-seeded record that repeats the same incorrect route string.
			record, found, err := state.NewIdempotencyStore(db).Lookup(t.Context(), tc.route, operatorActorID, key)
			if err != nil || !found || record.Status != state.IdempotencyStatusCompleted {
				t.Fatalf("journal route mismatch: found=%v err=%v", found, err)
			}
			path := "/mutation-outcomes/" + key + "?operation=" + tc.operation
			read := request(renewed, http.MethodGet, path, "", nil)
			if read.Code != http.StatusOK {
				t.Fatalf("renewed read: %d %s", read.Code, read.Body.String())
			}
			var outcome gen.MutationOutcome
			if err := json.Unmarshal(read.Body.Bytes(), &outcome); err != nil {
				t.Fatal(err)
			}
			if outcome.State != gen.MutationOutcomeStateCompleted || outcome.ResponseStatus == nil || *outcome.ResponseStatus != tc.status {
				t.Fatalf("wrong outcome: %+v", outcome)
			}
			if tc.status == http.StatusNoContent {
				if outcome.ResponseBody != nil || strings.Contains(read.Body.String(), `"response_body"`) {
					t.Fatal("204 response must omit body")
				}
			} else {
				var expected map[string]interface{}
				if err := json.Unmarshal(original.Body.Bytes(), &expected); err != nil {
					t.Fatal(err)
				}
				if outcome.ResponseBody == nil || !reflect.DeepEqual(*outcome.ResponseBody, expected) {
					t.Fatal("outcome changed original result")
				}
			}
			if strings.Contains(read.Body.String(), first.session.CSRFToken) || strings.Contains(read.Body.String(), renewed.session.CSRFToken) || strings.Contains(read.Body.String(), "response_headers") {
				t.Fatal("outcome exposed credentials or headers")
			}
			if anonymous := request(browser{}, http.MethodGet, path, "", nil); anonymous.Code != http.StatusUnauthorized {
				t.Fatalf("anonymous read: %d", anonymous.Code)
			}
			replay := request(renewed, tc.method, tc.path, key, body)
			if replay.Code != http.StatusConflict || !strings.Contains(replay.Body.String(), `"code":"idempotency_conflict"`) {
				t.Fatalf("renewed binding replayed mutation: %d %s", replay.Code, replay.Body.String())
			}
			if _, err := db.Exec(`UPDATE idempotency_keys SET actor_id='another-actor' WHERE client_key=? AND route=?`, key, tc.route); err != nil {
				t.Fatal(err)
			}
			other := request(renewed, http.MethodGet, path, "", nil)
			if other.Code != http.StatusOK || !strings.Contains(other.Body.String(), `"state":"unknown"`) || strings.Contains(other.Body.String(), "response_body") {
				t.Fatalf("cross-actor result: %d %s", other.Code, other.Body.String())
			}
		})
	}
}
