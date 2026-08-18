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
	flow, err := manager.Create("session-a", "https://kingdom.example", sessionExpiresAt)
	if err != nil {
		t.Fatal(err)
	}
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
	flow, err := server.manifestFlows.Create("browser-session", "https://kingdom.example", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/connections/github/manifest-flows/manifest-callback?state=" + flow.ManifestState + "&code=manifest-code"
	request := httptest.NewRequest(http.MethodGet, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "https://kingdom.example/connections" {
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
	hook, _ := manifest["hook_attributes"].(map[string]any)
	events, _ := manifest["default_events"].([]any)
	callbacks, _ := manifest["callback_urls"].([]any)
	if len(manifest) != 11 || manifest["public"] != false || manifest["request_oauth_on_install"] != false || manifest["setup_on_update"] != false || len(events) != 0 || len(permissions) != 2 || permissions["metadata"] != "read" || permissions["issues"] != "read" || hook["active"] != false || len(callbacks) != 1 {
		t.Fatalf("unexpected manifest: %#v", manifest)
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

func TestManifestFlowExpiryPrunesAndZeroesSecrets(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	manager := newManifestFlowManager(func() time.Time { return now })
	flow, _ := manager.Create("session", "https://kingdom.example", now.Add(time.Hour))
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
	if _, err := manager.Create("one", "https://one.example", expiresAt); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create("two", "https://two.example", expiresAt); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create("three", "https://three.example", expiresAt); !errors.Is(err, errManifestFlowCapacity) {
		t.Fatalf("capacity error=%v", err)
	}
}

func TestManifestFlowRetentionEvictsOldestTerminalAndPreservesActive(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	manager := newManifestFlowManager(func() time.Time { return now })
	manager.maxRetained = 4
	active, err := manager.Create("active", "https://kingdom.example", now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	var firstTerminalID string
	var firstSecret []byte
	for i := 0; i < 100; i++ {
		now = now.Add(time.Second)
		flow, createErr := manager.Create("terminal", "https://kingdom.example", now.Add(time.Hour))
		if createErr != nil {
			t.Fatalf("create terminal flow %d: %v", i, createErr)
		}
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
