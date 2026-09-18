package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"kingdom/internal/external"
	githubexternal "kingdom/internal/external/github"
)

func TestGitHubManifestAuthenticationFailureIsNotLocalUnauthorized(t *testing.T) {
	response := httptest.NewRecorder()
	writeGitHubSetupError(response, "provider details must stay hidden", &external.ProviderError{Class: external.FailureAuthentication, Status: http.StatusUnauthorized})
	if response.Code != http.StatusFailedDependency || !strings.Contains(response.Body.String(), `"provider_failure_class":"revoked"`) || !strings.Contains(strings.ToLower(response.Body.String()), "reconnect") {
		t.Fatalf("manifest authentication response status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestContextWithFlowDeadlineUsesInjectedClock(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	expired, cancelExpired := contextWithFlowDeadline(context.Background(), func() time.Time { return now }, now)
	defer cancelExpired()
	select {
	case <-expired.Done():
	default:
		t.Fatal("expired flow context remained active")
	}

	active, cancelActive := contextWithFlowDeadline(context.Background(), func() time.Time { return now }, now.Add(time.Minute))
	defer cancelActive()
	select {
	case <-active.Done():
		t.Fatal("active flow context used wall clock instead of injected clock")
	default:
	}
}

func TestManifestFlowStagesBindSessionOriginAndRejectReplay(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	manager := newManifestFlowManager(func() time.Time { return now })
	sessionExpiresAt := now.Add(30 * time.Minute)
	flow := createManifestFlow(t, manager, "session-a", "https://kingdom.example", sessionExpiresAt)
	if !flow.ExpiresAt.Equal(sessionExpiresAt) {
		t.Fatalf("manifest flow expiry=%s, want session expiry %s", flow.ExpiresAt, sessionExpiresAt)
	}
	if _, err := manager.Status(flow.ID, "session-b", flow.Origin); !errors.Is(err, errManifestFlowNotFound) {
		t.Fatalf("cross-session status error=%v", err)
	}
	if _, err := manager.Status(flow.ID, flow.SessionID, "https://other.example"); !errors.Is(err, errManifestFlowNotFound) {
		t.Fatalf("cross-origin status error=%v", err)
	}
	if _, err := manager.RecordManifestCode(flow.ManifestState, "manifest-code"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.RecordManifestCode(flow.ManifestState, "replay"); !errors.Is(err, errManifestFlowNotFound) {
		t.Fatalf("manifest state replay error=%v", err)
	}
	conversion, err := manager.BeginConversion(flow.ID, flow.SessionID, flow.Origin)
	if err != nil || string(conversion.Code) != "manifest-code" || !conversion.ExpiresAt.Equal(flow.ExpiresAt) {
		t.Fatalf("BeginConversion() lease=%#v err=%v", conversion, err)
	}
	converted := githubexternal.ManifestConversion{AppID: "1", ClientID: "Iv1", AppSlug: "kingdom-issues", Name: "Kingdom Issues", PrivateKey: []byte("pem"), ClientSecret: []byte("secret")}
	if err := manager.CompleteConversion(flow.ID, converted); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.RecordInstallation(flow.SetupState, "2", "install"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.RecordInstallation(flow.SetupState, "2", "install"); !errors.Is(err, errManifestFlowNotFound) {
		t.Fatalf("setup state replay error=%v", err)
	}
	oauth, err := manager.OAuthRequest(flow.ID, flow.SessionID, flow.Origin)
	if err != nil || oauth.State == "" || len(oauth.Challenge) != 43 {
		t.Fatalf("OAuthRequest()=%#v err=%v", oauth, err)
	}
	if _, err := manager.RecordOAuthCode(oauth.State, "oauth-code"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.RecordOAuthCode(oauth.State, "replay"); !errors.Is(err, errManifestFlowNotFound) {
		t.Fatalf("oauth state replay error=%v", err)
	}
	completion, err := manager.BeginCompletion(flow.ID, flow.SessionID, flow.Origin)
	if err != nil || string(completion.ClientSecret) != "secret" || string(completion.OAuthCode) != "oauth-code" {
		t.Fatalf("BeginCompletion()=%#v err=%v", completion, err)
	}
	if _, err := manager.BeginCompletion(flow.ID, flow.SessionID, flow.Origin); !errors.Is(err, errManifestFlowStage) {
		t.Fatalf("completion replay error=%v", err)
	}
	if err := manager.Finalize(flow.ID, "connection-1", "10", "octocat"); err != nil {
		t.Fatal(err)
	}
	status, err := manager.Status(flow.ID, flow.SessionID, flow.Origin)
	if err != nil || status.Stage != manifestStageFinalized || status.ConnectionID != "connection-1" {
		t.Fatalf("final status=%#v err=%v", status, err)
	}
}

func TestManifestFlowStartReservationResumesSameIntentAndConflictsWithAnother(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	manager := newManifestFlowManager(func() time.Time { return now })
	first, created, err := manager.ReserveStart("session", "https://kingdom.example", "organization", "kingdom-org", now.Add(time.Hour))
	if err != nil || !created {
		t.Fatalf("first reservation created=%v err=%v", created, err)
	}
	resumed, created, err := manager.ReserveStart("session", "https://kingdom.example", "organization", "kingdom-org", now.Add(time.Hour))
	if err != nil || created || resumed.ID != first.ID || resumed.ManifestState != first.ManifestState {
		t.Fatalf("same intent did not resume: flow=%#v created=%v err=%v", resumed, created, err)
	}
	if _, _, err := manager.ReserveStart("session", "https://kingdom.example", "personal", "", now.Add(time.Hour)); !errors.Is(err, errManifestFlowIntentConflict) {
		t.Fatalf("personal intent while organization flow is active error=%v", err)
	}
	if _, _, err := manager.ReserveStart("session", "https://kingdom.example", "organization", "other-org", now.Add(time.Hour)); !errors.Is(err, errManifestFlowIntentConflict) {
		t.Fatalf("different organization intent error=%v", err)
	}
	if err := manager.Discard(first.ID, first.SessionID, first.Origin); err != nil {
		t.Fatal(err)
	}
	if _, created, err := manager.ReserveStart("session", "https://kingdom.example", "personal", "", now.Add(time.Hour)); err != nil || !created {
		t.Fatalf("new intent after discard created=%v err=%v", created, err)
	}
}

func TestManifestFlowFinalizationRemainsPinnedAcrossExpiry(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	manager := newManifestFlowManager(func() time.Time { return now })
	manager.flows["flow-finalizing"] = &githubManifestFlow{ID: "flow-finalizing", SessionID: "session", Origin: "https://kingdom.example", Stage: manifestStageFinalizing, ExpiresAt: now.Add(time.Minute)}

	now = now.Add(2 * time.Minute)
	if _, err := manager.Status("flow-finalizing", "session", "https://kingdom.example"); !errors.Is(err, errManifestFlowNotFound) {
		t.Fatalf("expired finalizing status error=%v", err)
	}
	if err := manager.Finalize("flow-finalizing", "connection", "10", "octocat"); err != nil {
		t.Fatalf("finalizing flow was pruned during durable completion: %v", err)
	}
}

func TestGitHubManifestCallbackUsesOneTimeStateWithoutCookie(t *testing.T) {
	handler, _ := newAPIServerForTest(t)
	server := handler.(*Server)
	flow := createManifestFlow(t, server.manifestFlows, "browser-session", "https://kingdom.example", time.Now().Add(time.Hour))
	path := "/api/v1/connections/github/manifest-flows/manifest-callback?state=" + flow.ManifestState + "&code=manifest-code"
	request := httptest.NewRequest(http.MethodGet, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	// The setup tab must land on the chromeless return page, not on the app, so
	// it cannot become a second driver of the same flow.
	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "https://kingdom.example/connections/github/return" {
		t.Fatalf("callback status=%d location=%q body=%s", response.Code, response.Header().Get("Location"), response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("Referrer-Policy") != "no-referrer" {
		t.Fatalf("callback security headers=%v", response.Header())
	}
	replay := httptest.NewRecorder()
	handler.ServeHTTP(replay, httptest.NewRequest(http.MethodGet, path, nil))
	if replay.Code != http.StatusBadRequest {
		t.Fatalf("callback replay status=%d body=%s", replay.Code, replay.Body.String())
	}
}

func TestStartGitHubManifestFlowBuildsExactPrivateAppManifestWithoutSensitiveIdempotencyPayload(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	server := handler.(*Server)
	cookie, session := seedBrowserSessionForTest(t, server, "http://127.0.0.1:9090", server.now().UTC().Add(time.Hour))
	body := bytes.NewBufferString(`{"owner_type":"organization","organization":"kingdom-org"}`)
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:9090/api/v1/connections/github/manifest-flows", body)
	request.RemoteAddr = "127.0.0.1:49152"
	request.Host = "127.0.0.1:9090"
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", "http://127.0.0.1:9090")
	request.Header.Set("X-CSRF-Token", session.CSRFToken)
	request.Header.Set("Idempotency-Key", testIdempotencyKey(t, "manifest-start", time.Now()))
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("start status=%d body=%s", response.Code, response.Body.String())
	}
	var started startManifestFlowResponse
	if err := json.Unmarshal(response.Body.Bytes(), &started); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(started.FormAction, "https://github.com/organizations/kingdom-org/settings/apps/new?state=") {
		t.Fatalf("organization form action=%q", started.FormAction)
	}
	var manifest map[string]any
	if err := json.Unmarshal([]byte(started.Manifest), &manifest); err != nil {
		t.Fatal(err)
	}
	permissions, _ := manifest["default_permissions"].(map[string]any)
	events, _ := manifest["default_events"].([]any)
	callbacks, _ := manifest["callback_urls"].([]any)
	if len(manifest) != 10 || manifest["public"] != false || manifest["request_oauth_on_install"] != false || manifest["setup_on_update"] != false || len(events) != 0 || len(permissions) != 2 || permissions["metadata"] != "read" || permissions["issues"] != "read" || len(callbacks) != 1 {
		t.Fatalf("unexpected manifest: %#v", manifest)
	}
	// GitHub rejects a manifest whose hook URL is not publicly reachable, even
	// when the hook is inactive, so a localhost install can only create an App
	// when hook_attributes is omitted entirely.
	if _, present := manifest["hook_attributes"]; present {
		t.Fatalf("manifest declares hook_attributes: %#v", manifest["hook_attributes"])
	}
	for _, key := range []string{"url", "redirect_url", "setup_url"} {
		if !strings.HasPrefix(manifest[key].(string), "http://127.0.0.1:9090") {
			t.Fatalf("manifest %s=%q", key, manifest[key])
		}
	}
	var status int
	var headers, responseBody string
	if err := db.QueryRow(`SELECT response_status, response_headers_json, response_body_json FROM idempotency_keys WHERE route LIKE '%manifest-flows%'`).Scan(&status, &headers, &responseBody); err != nil {
		t.Fatalf("read manifest idempotency metadata: %v", err)
	}
	if status != 0 || headers != "{}" || responseBody != "" {
		t.Fatalf("manifest response persisted: status=%d headers=%q body=%q", status, headers, responseBody)
	}
}

func TestStartGitHubManifestFlowResumesOnlyTheSameIntent(t *testing.T) {
	handler, _ := newAPIServerForTest(t)
	server := handler.(*Server)
	cookie, session := seedBrowserSessionForTest(t, server, "http://127.0.0.1:9090", server.now().UTC().Add(time.Hour))

	start := func(key, body string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:9090/api/v1/connections/github/manifest-flows", bytes.NewBufferString(body))
		request.RemoteAddr = "127.0.0.1:49152"
		request.Host = "127.0.0.1:9090"
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Origin", "http://127.0.0.1:9090")
		request.Header.Set("X-CSRF-Token", session.CSRFToken)
		request.Header.Set("Idempotency-Key", key)
		request.AddCookie(cookie)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}

	organization := `{"owner_type":"organization","organization":"kingdom-org"}`
	key := testIdempotencyKey(t, "manifest-reservation", time.Now())
	first := start(key, organization)
	if first.Code != http.StatusCreated {
		t.Fatalf("first start status=%d body=%s", first.Code, first.Body.String())
	}
	var started startManifestFlowResponse
	if err := json.Unmarshal(first.Body.Bytes(), &started); err != nil {
		t.Fatal(err)
	}
	replay := start(key, organization)
	if replay.Code != http.StatusCreated || replay.Body.String() != first.Body.String() {
		t.Fatalf("idempotency replay status=%d body=%s, want status=%d body=%s", replay.Code, replay.Body.String(), first.Code, first.Body.String())
	}
	resumed := start(testIdempotencyKey(t, "manifest-reservation-resume", time.Now()), organization)
	var resumedStatus manifestFlowStatus
	if resumed.Code != http.StatusOK || json.Unmarshal(resumed.Body.Bytes(), &resumedStatus) != nil || resumedStatus.FlowID != started.FlowID || resumedStatus.Stage != manifestStageAwaitingManifest {
		t.Fatalf("same intent resume status=%d body=%s", resumed.Code, resumed.Body.String())
	}
	conflict := start(testIdempotencyKey(t, "manifest-reservation-conflict", time.Now()), `{"owner_type":"personal"}`)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("competing intent status=%d body=%s", conflict.Code, conflict.Body.String())
	}
}

func TestManifestFlowExpiryPrunesAndZeroesSecrets(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	manager := newManifestFlowManager(func() time.Time { return now })
	flow := createManifestFlow(t, manager, "session", "https://kingdom.example", now.Add(time.Hour))
	_, _ = manager.RecordManifestCode(flow.ManifestState, "manifest-code")
	secret := manager.flows[flow.ID].ManifestCode
	now = now.Add(time.Hour)
	if _, err := manager.Current(flow.SessionID, flow.Origin); !errors.Is(err, errManifestFlowNotFound) {
		t.Fatalf("expired flow error=%v", err)
	}
	for i, value := range secret {
		if value != 0 {
			t.Fatalf("expired secret byte %d was not zeroed", i)
		}
	}
}

func TestManifestFlowCapacityIsBounded(t *testing.T) {
	manager := newManifestFlowManager(time.Now)
	manager.maxRetained = 2
	expiresAt := time.Now().Add(time.Hour)
	createManifestFlow(t, manager, "one", "https://one.example", expiresAt)
	createManifestFlow(t, manager, "two", "https://two.example", expiresAt)
	if _, _, err := manager.ReserveStart("three", "https://three.example", "personal", "", expiresAt); !errors.Is(err, errManifestFlowCapacity) {
		t.Fatalf("capacity error=%v", err)
	}
}

func TestManifestFlowRetentionEvictsOldestTerminalAndPreservesActive(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	manager := newManifestFlowManager(func() time.Time { return now })
	manager.maxRetained = 4
	active := createManifestFlow(t, manager, "active", "https://kingdom.example", now.Add(time.Hour))

	var firstTerminalID string
	var firstSecret []byte
	for i := 0; i < 100; i++ {
		now = now.Add(time.Second)
		flow := createManifestFlow(t, manager, "terminal", "https://kingdom.example", now.Add(time.Hour))
		secret := []byte(fmt.Sprintf("secret-%d", i))
		manager.flows[flow.ID].PrivateKey = secret
		if i%2 == 0 {
			manager.Fail(flow.ID, "failed")
		} else {
			manager.flows[flow.ID].Stage = manifestStageFinalizing
			if finalizeErr := manager.Finalize(flow.ID, fmt.Sprintf("connection-%d", i), "10", "octocat"); finalizeErr != nil {
				t.Fatalf("finalize flow %d: %v", i, finalizeErr)
			}
		}
		if i == 0 {
			firstTerminalID, firstSecret = flow.ID, secret
		}
		if len(manager.flows) > manager.maxRetained {
			t.Fatalf("retained %d manifest flows, max %d", len(manager.flows), manager.maxRetained)
		}
		for index, value := range secret {
			if value != 0 {
				t.Fatalf("terminal secret %d byte %d was not zeroed", i, index)
			}
		}
	}

	if manager.flows[active.ID] == nil {
		t.Fatal("active manifest flow was evicted under terminal pressure")
	}
	if manager.flows[firstTerminalID] != nil {
		t.Fatal("oldest terminal manifest flow was retained")
	}
	for index, value := range firstSecret {
		if value != 0 {
			t.Fatalf("evicted manifest secret byte %d was not zeroed", index)
		}
	}
	if len(manager.states) > manager.maxRetained*3 {
		t.Fatalf("manifest state map retained %d entries", len(manager.states))
	}
}

func TestManifestFlowDiscardIsSessionBoundAndZeroesSecrets(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	manager := newManifestFlowManager(func() time.Time { return now })
	flow := createManifestFlow(t, manager, "session-a", "https://kingdom.example", now.Add(30*time.Minute))
	if _, err := manager.RecordManifestCode(flow.ManifestState, "manifest-code"); err != nil {
		t.Fatal(err)
	}
	secret := manager.flows[flow.ID].ManifestCode

	if err := manager.Discard(flow.ID, "session-b", flow.Origin); !errors.Is(err, errManifestFlowNotFound) {
		t.Fatalf("cross-session discard error=%v", err)
	}
	if err := manager.Discard(flow.ID, flow.SessionID, "https://other.example"); !errors.Is(err, errManifestFlowNotFound) {
		t.Fatalf("cross-origin discard error=%v", err)
	}
	if err := manager.Discard(flow.ID, flow.SessionID, flow.Origin); err != nil {
		t.Fatal(err)
	}

	for _, value := range secret {
		if value != 0 {
			t.Fatalf("discarded flow retained manifest code %q", secret)
		}
	}
	if _, err := manager.Status(flow.ID, flow.SessionID, flow.Origin); !errors.Is(err, errManifestFlowNotFound) {
		t.Fatalf("status after discard error=%v", err)
	}
	if _, err := manager.Current(flow.SessionID, flow.Origin); !errors.Is(err, errManifestFlowNotFound) {
		t.Fatalf("current after discard error=%v", err)
	}
	if _, err := manager.RecordManifestCode(flow.ManifestState, "replay"); !errors.Is(err, errManifestFlowNotFound) {
		t.Fatalf("state replay after discard error=%v", err)
	}
	if err := manager.Discard(flow.ID, flow.SessionID, flow.Origin); !errors.Is(err, errManifestFlowNotFound) {
		t.Fatalf("repeat discard error=%v", err)
	}
}

func TestManifestFlowDiscardRejectsInFlightWork(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	manager := newManifestFlowManager(func() time.Time { return now })
	flow := createManifestFlow(t, manager, "session-a", "https://kingdom.example", now.Add(30*time.Minute))
	if _, err := manager.RecordManifestCode(flow.ManifestState, "manifest-code"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.BeginConversion(flow.ID, flow.SessionID, flow.Origin); err != nil {
		t.Fatal(err)
	}
	if err := manager.Discard(flow.ID, flow.SessionID, flow.Origin); !errors.Is(err, errManifestFlowInFlight) {
		t.Fatalf("discard converting flow error=%v", err)
	}
	if err := manager.CompleteConversion(flow.ID, githubexternal.ManifestConversion{}); err != nil {
		t.Fatalf("complete conversion after rejected discard: %v", err)
	}

	manager.flows[flow.ID].Stage = manifestStageFinalizing
	if err := manager.Discard(flow.ID, flow.SessionID, flow.Origin); !errors.Is(err, errManifestFlowInFlight) {
		t.Fatalf("discard finalizing flow error=%v", err)
	}
	if err := manager.Finalize(flow.ID, "connection-1", "account-1", "octocat"); err != nil {
		t.Fatalf("finalize after rejected discard: %v", err)
	}
}

func TestDiscardGitHubManifestFlowEndpointAbandonsResumableSetup(t *testing.T) {
	handler, _ := newAPIServerForTest(t)
	server := handler.(*Server)
	cookie, session := seedBrowserSessionForTest(t, server, "http://127.0.0.1:9090", server.now().UTC().Add(time.Hour))

	start := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:9090/api/v1/connections/github/manifest-flows", bytes.NewBufferString(`{"owner_type":"personal"}`))
	start.RemoteAddr = "127.0.0.1:49152"
	start.Host = "127.0.0.1:9090"
	start.Header.Set("Content-Type", "application/json")
	start.Header.Set("Origin", "http://127.0.0.1:9090")
	start.Header.Set("X-CSRF-Token", session.CSRFToken)
	start.Header.Set("Idempotency-Key", testIdempotencyKey(t, "manifest-start", time.Now()))
	start.AddCookie(cookie)
	startResponse := httptest.NewRecorder()
	handler.ServeHTTP(startResponse, start)
	if startResponse.Code != http.StatusCreated {
		t.Fatalf("start status=%d body=%s", startResponse.Code, startResponse.Body.String())
	}
	var started startManifestFlowResponse
	if err := json.Unmarshal(startResponse.Body.Bytes(), &started); err != nil {
		t.Fatal(err)
	}

	// Distinct logical keys: an identical key is a replay the middleware serves
	// from its store, which would hide the handler's own behaviour.
	discard := func(logicalKey string, authenticated bool) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodDelete, "http://127.0.0.1:9090/api/v1/connections/github/manifest-flows/"+started.FlowID, nil)
		request.RemoteAddr = "127.0.0.1:49152"
		request.Host = "127.0.0.1:9090"
		request.Header.Set("Origin", "http://127.0.0.1:9090")
		request.Header.Set("Idempotency-Key", testIdempotencyKey(t, logicalKey, time.Now()))
		if authenticated {
			request.Header.Set("X-CSRF-Token", session.CSRFToken)
			request.AddCookie(cookie)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}

	if response := discard("manifest-discard-anonymous", false); response.Code == http.StatusNoContent {
		t.Fatalf("discard without a browser session succeeded: %d", response.Code)
	}
	server.manifestFlows.flows[started.FlowID].Stage = manifestStageConverting
	if response := discard("manifest-discard-in-flight", true); response.Code != http.StatusConflict {
		t.Fatalf("in-flight discard status=%d body=%s", response.Code, response.Body.String())
	}
	server.manifestFlows.flows[started.FlowID].Stage = manifestStageAwaitingManifest
	if response := discard("manifest-discard", true); response.Code != http.StatusNoContent {
		t.Fatalf("discard status=%d body=%s", response.Code, response.Body.String())
	}

	// An abandoned flow must stop presenting itself as resumable setup state.
	current := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9090/api/v1/connections/github/manifest-flows", nil)
	current.RemoteAddr = "127.0.0.1:49152"
	current.Host = "127.0.0.1:9090"
	current.AddCookie(cookie)
	currentResponse := httptest.NewRecorder()
	handler.ServeHTTP(currentResponse, current)
	if currentResponse.Code != http.StatusNoContent {
		t.Fatalf("current flow after discard status=%d body=%s", currentResponse.Code, currentResponse.Body.String())
	}
	if response := discard("manifest-discard-repeat", true); response.Code != http.StatusNotFound {
		t.Fatalf("repeat discard status=%d body=%s", response.Code, response.Body.String())
	}
}

func createManifestFlow(t *testing.T, manager *manifestFlowManager, sessionID, origin string, expiresAt time.Time) *githubManifestFlow {
	t.Helper()
	flow, created, err := manager.ReserveStart(sessionID, origin, "personal", "", expiresAt)
	if err != nil || !created {
		t.Fatalf("create manifest flow created=%v err=%v", created, err)
	}
	return flow
}
