package api

import (
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

func TestDeviceFlowManagerEnforcesPacingBindingAndSingleFinalization(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	manager := newDeviceFlowManager(func() time.Time { return now })
	sessionExpiresAt := now.Add(10 * time.Minute)
	flow, err := manager.Create("session-1", "https://kingdom.example", "", sessionExpiresAt, githubexternal.DeviceAuthorization{DeviceCode: "provider-secret", UserCode: "ABCD-EFGH", VerificationURI: "https://github.com/login/device", ExpiresAt: now.Add(15 * time.Minute), Interval: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if !flow.ExpiresAt.Equal(sessionExpiresAt) {
		t.Fatalf("device flow expiry=%s, want session expiry %s", flow.ExpiresAt, sessionExpiresAt)
	}
	if _, _, err := manager.BeginPoll(flow.ID, "other-session", flow.Origin); !errors.Is(err, errDeviceFlowStage) {
		t.Fatalf("cross-session poll error=%v", err)
	}
	if _, retryAt, err := manager.BeginPoll(flow.ID, flow.SessionID, flow.Origin); !errors.Is(err, errDeviceFlowPaced) || retryAt == nil || !retryAt.Equal(now.Add(5*time.Second)) {
		t.Fatalf("early poll retry=%v err=%v", retryAt, err)
	}
	now = now.Add(5 * time.Second)
	lease, _, err := manager.BeginPoll(flow.ID, flow.SessionID, flow.Origin)
	if err != nil || lease.DeviceCode != "provider-secret" {
		t.Fatalf("poll lease=%#v err=%v", lease, err)
	}
	tokens := githubexternal.UserTokens{AccessToken: "access-token", RefreshToken: "refresh-token", AccessExpiresAt: now.Add(time.Hour), RefreshExpiresAt: now.Add(180 * 24 * time.Hour)}
	candidate := githubexternal.InstallationCandidate{InstallationID: "2", DisplayName: "octocat", AccountID: "10", AccountLogin: "octocat", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom"}
	if err := manager.CompletePoll(flow.ID, githubexternal.DevicePollResult{Status: githubexternal.DevicePollAuthorized, Tokens: tokens}, []githubexternal.InstallationCandidate{candidate}); err != nil {
		t.Fatal(err)
	}
	status, err := manager.Status(flow.ID, flow.SessionID, flow.Origin, "kingdom")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(status)
	for _, secret := range []string{"provider-secret", "access-token", "refresh-token", "device_code", "access_token", "refresh_token"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("safe device status leaked %q: %s", secret, raw)
		}
	}
	access, err := manager.AccessTokenForFinalize(flow.ID, flow.SessionID, flow.Origin)
	if err != nil || access.AccessToken != "access-token" || !access.ExpiresAt.Equal(flow.ExpiresAt) {
		t.Fatalf("finalize access=%#v err=%v", access, err)
	}
	// The client cannot invent an installation the fresh listing does not carry.
	if _, _, err := manager.BeginFinalize(flow.ID, flow.SessionID, flow.Origin, []string{"999"}, []githubexternal.InstallationCandidate{candidate}); !errors.Is(err, errDeviceFlowStage) {
		t.Fatalf("spoofed candidate error=%v", err)
	}
	if _, err := manager.AccessTokenForFinalize(flow.ID, flow.SessionID, flow.Origin); err != nil {
		t.Fatalf("reacquire finalize verification after rejected candidate: %v", err)
	}
	finalize, selected, err := manager.BeginFinalize(flow.ID, flow.SessionID, flow.Origin, []string{"2"}, []githubexternal.InstallationCandidate{candidate})
	if err != nil || len(selected) != 1 || selected[0].AccountID != "10" || finalize.Tokens.AccessToken != "access-token" {
		t.Fatalf("finalize=%#v candidate=%#v err=%v", finalize, selected, err)
	}
	if _, _, err := manager.BeginFinalize(flow.ID, flow.SessionID, flow.Origin, []string{"2"}, []githubexternal.InstallationCandidate{candidate}); !errors.Is(err, errDeviceFlowStage) {
		t.Fatalf("replay finalize error=%v", err)
	}
}

func TestDeviceFlowFinalizationRemainsPinnedAcrossExpiry(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	manager := newDeviceFlowManager(func() time.Time { return now })
	manager.flows["flow-finalizing"] = &githubDeviceFlow{ID: "flow-finalizing", SessionID: "session", Origin: "https://kingdom.example", Stage: deviceStageFinalizing, ExpiresAt: now.Add(time.Minute), InFlight: true}

	now = now.Add(2 * time.Minute)
	if _, err := manager.Status("flow-finalizing", "session", "https://kingdom.example", "kingdom"); !errors.Is(err, errDeviceFlowNotFound) {
		t.Fatalf("expired finalizing status error=%v", err)
	}
	if err := manager.Finalize("flow-finalizing", "connection"); err != nil {
		t.Fatalf("finalizing flow was pruned during durable completion: %v", err)
	}
}

func TestGitHubDeviceErrorsExposeOnlySafeProviderClassification(t *testing.T) {
	retryAt := time.Date(2026, 8, 15, 12, 1, 0, 0, time.UTC)
	for _, test := range []struct {
		name   string
		class  external.FailureClass
		status int
	}{
		{name: "authentication", class: external.FailureAuthentication, status: http.StatusFailedDependency},
		{name: "rate limit", class: external.FailureRateLimited, status: http.StatusTooManyRequests},
		{name: "timeout", class: external.FailureTimeout, status: http.StatusGatewayTimeout},
		{name: "unavailable", class: external.FailureUnavailable, status: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			providerErr := &external.ProviderError{Class: test.class}
			if test.class == external.FailureRateLimited {
				providerErr.RetryAt = &retryAt
			}
			err := fmt.Errorf("provider-body-secret: %w", providerErr)
			response := httptest.NewRecorder()
			writeGitHubDeviceError(response, "GitHub setup failed safely", err)
			if response.Code != test.status || strings.Contains(response.Body.String(), "provider-body-secret") {
				t.Fatalf("unsafe device error status=%d body=%s", response.Code, response.Body.String())
			}
			if test.class == external.FailureAuthentication && (!strings.Contains(response.Body.String(), `"provider_failure_class":"revoked"`) || !strings.Contains(strings.ToLower(response.Body.String()), "reconnect")) {
				t.Fatalf("authentication recovery guidance missing: %s", response.Body.String())
			}
			if test.class == external.FailureRateLimited && !strings.Contains(response.Body.String(), retryAt.Format(time.RFC3339)) {
				t.Fatalf("rate-limit retry time missing: %s", response.Body.String())
			}
		})
	}
}

func TestDeviceFlowRetentionEvictsOldestTerminalAndPreservesActive(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	manager := newDeviceFlowManager(func() time.Time { return now })
	manager.maxRetained = 4
	authorization := func(secret string) githubexternal.DeviceAuthorization {
		return githubexternal.DeviceAuthorization{DeviceCode: secret, UserCode: "ABCD-EFGH", VerificationURI: "https://github.com/login/device", ExpiresAt: now.Add(time.Hour), Interval: time.Second}
	}
	active, err := manager.Create("active", "https://kingdom.example", "", now.Add(2*time.Hour), authorization("active-secret"))
	if err != nil {
		t.Fatal(err)
	}

	var firstTerminalID string
	for i := 0; i < 100; i++ {
		now = now.Add(time.Second)
		flow, createErr := manager.Create("terminal", "https://kingdom.example", "", now.Add(2*time.Hour), authorization(fmt.Sprintf("device-secret-%d", i)))
		if createErr != nil {
			t.Fatalf("create terminal flow %d: %v", i, createErr)
		}
		if i%2 == 0 {
			manager.Fail(flow.ID, "failed")
		} else {
			manager.flows[flow.ID].Stage = deviceStageFinalizing
			manager.flows[flow.ID].Tokens = githubexternal.UserTokens{AccessToken: "access-secret", RefreshToken: "refresh-secret"}
			if finalizeErr := manager.Finalize(flow.ID, fmt.Sprintf("connection-%d", i)); finalizeErr != nil {
				t.Fatalf("finalize flow %d: %v", i, finalizeErr)
			}
		}
		if i == 0 {
			firstTerminalID = flow.ID
		}
		if len(manager.flows) > manager.maxRetained {
			t.Fatalf("retained %d device flows, max %d", len(manager.flows), manager.maxRetained)
		}
		retained := manager.flows[flow.ID]
		if retained != nil && (retained.DeviceCode != "" || retained.UserCode != "" || retained.Tokens.AccessToken != "" || retained.Tokens.RefreshToken != "" || retained.Candidates != nil) {
			t.Fatalf("terminal device flow retained secrets: %#v", retained)
		}
	}

	if manager.flows[active.ID] == nil || manager.flows[active.ID].DeviceCode != "active-secret" {
		t.Fatal("active device flow was evicted or cleared under terminal pressure")
	}
	if manager.flows[firstTerminalID] != nil {
		t.Fatal("oldest terminal device flow was retained")
	}
}

func TestDeviceFlowExposesInstallURLWhileChoosingAmongExistingInstallations(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	const wantInstallURL = "https://github.com/apps/kingdom-issue-importer/installations/new"
	tokens := githubexternal.UserTokens{AccessToken: "access-token", RefreshToken: "refresh-token", AccessExpiresAt: now.Add(time.Hour), RefreshExpiresAt: now.Add(180 * 24 * time.Hour)}
	candidate := githubexternal.InstallationCandidate{InstallationID: "2", DisplayName: "octocat", AccountID: "10", AccountLogin: "octocat", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom-issue-importer", RepositorySelection: "selected"}

	for _, test := range []struct {
		name       string
		candidates []githubexternal.InstallationCandidate
		wantStage  deviceFlowStage
	}{
		{name: "no installation yet", candidates: nil, wantStage: deviceStageNeedsInstallation},
		{name: "already has an installation", candidates: []githubexternal.InstallationCandidate{candidate}, wantStage: deviceStageSelecting},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := newDeviceFlowManager(func() time.Time { return now })
			flow, err := manager.Create("session-1", "https://kingdom.example", "", now.Add(10*time.Minute), githubexternal.DeviceAuthorization{DeviceCode: "provider-secret", UserCode: "ABCD-EFGH", VerificationURI: "https://github.com/login/device", ExpiresAt: now.Add(15 * time.Minute), Interval: 0})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := manager.BeginPoll(flow.ID, flow.SessionID, flow.Origin); err != nil {
				t.Fatal(err)
			}
			if err := manager.CompletePoll(flow.ID, githubexternal.DevicePollResult{Status: githubexternal.DevicePollAuthorized, Tokens: tokens}, test.candidates); err != nil {
				t.Fatal(err)
			}
			status, err := manager.Status(flow.ID, flow.SessionID, flow.Origin, "kingdom-issue-importer")
			if err != nil {
				t.Fatal(err)
			}
			if status.Stage != test.wantStage {
				t.Fatalf("stage=%q, want %q", status.Stage, test.wantStage)
			}
			if status.InstallURL != wantInstallURL {
				t.Fatalf("install_url=%q, want %q so the operator can always install on another account", status.InstallURL, wantInstallURL)
			}
		})
	}
}

func TestDeviceFlowCandidateRefreshKeepsInstallURLAndRepositoryScope(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	manager := newDeviceFlowManager(func() time.Time { return now })
	flow, err := manager.Create("session-1", "https://kingdom.example", "", now.Add(10*time.Minute), githubexternal.DeviceAuthorization{DeviceCode: "provider-secret", UserCode: "ABCD-EFGH", VerificationURI: "https://github.com/login/device", ExpiresAt: now.Add(15 * time.Minute), Interval: 0})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := manager.BeginPoll(flow.ID, flow.SessionID, flow.Origin); err != nil {
		t.Fatal(err)
	}
	tokens := githubexternal.UserTokens{AccessToken: "access-token", RefreshToken: "refresh-token", AccessExpiresAt: now.Add(time.Hour), RefreshExpiresAt: now.Add(180 * 24 * time.Hour)}
	first := githubexternal.InstallationCandidate{InstallationID: "2", DisplayName: "octocat", AccountID: "10", AccountLogin: "octocat", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom-issue-importer", RepositorySelection: "selected"}
	if err := manager.CompletePoll(flow.ID, githubexternal.DevicePollResult{Status: githubexternal.DevicePollAuthorized, Tokens: tokens}, []githubexternal.InstallationCandidate{first}); err != nil {
		t.Fatal(err)
	}

	// Installing on a second account mid-flow must be discoverable without repeating authorization.
	if _, err := manager.BeginCandidateRefresh(flow.ID, flow.SessionID, flow.Origin); err != nil {
		t.Fatalf("refresh from selecting_installation: %v", err)
	}
	second := githubexternal.InstallationCandidate{InstallationID: "3", DisplayName: "acme", AccountID: "11", AccountLogin: "acme", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom-issue-importer", RepositorySelection: "all"}
	if err := manager.CompleteCandidateRefresh(flow.ID, []githubexternal.InstallationCandidate{first, second}); err != nil {
		t.Fatal(err)
	}
	status, err := manager.Status(flow.ID, flow.SessionID, flow.Origin, "kingdom-issue-importer")
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Candidates) != 2 {
		t.Fatalf("candidates=%d, want 2 after installing on another account", len(status.Candidates))
	}
	if status.Candidates[0].RepositorySelection != "selected" || status.Candidates[1].RepositorySelection != "all" {
		t.Fatalf("repository scope not carried to the picker: %#v", status.Candidates)
	}
	if status.InstallURL == "" {
		t.Fatal("install_url missing after refresh; the operator cannot install on a further account")
	}
}
