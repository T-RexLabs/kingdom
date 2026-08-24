package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"kingdom/internal/external"
	githubexternal "kingdom/internal/external/github"
	"kingdom/internal/state"
)

func createDeviceFlow(t *testing.T, manager *deviceFlowManager, sessionID, origin, reconnectID string, sessionExpiresAt time.Time, authorization githubexternal.DeviceAuthorization) *githubDeviceFlow {
	t.Helper()
	_, lease, created, err := manager.ReserveStart(sessionID, origin, reconnectID, sessionExpiresAt)
	if err != nil || !created {
		t.Fatalf("reserve device flow: created=%v err=%v", created, err)
	}
	flow, err := manager.CompleteStart(lease, authorization)
	if err != nil {
		t.Fatalf("complete device flow start: %v", err)
	}
	return flow
}

func assertRawEmptyDeviceFlowCandidates(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode device flow response: %v; body=%s", err, response.Body.String())
	}
	if got := string(payload["candidates"]); got != "[]" {
		t.Fatalf("device flow candidates must be a raw empty array, got %s; body=%s", got, response.Body.String())
	}
}

func TestDeviceFlowManagerEnforcesPacingBindingAndSingleFinalization(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	manager := newDeviceFlowManager(func() time.Time { return now })
	sessionExpiresAt := now.Add(10 * time.Minute)
	flow := createDeviceFlow(t, manager, "session-1", "https://kingdom.example", "", sessionExpiresAt, githubexternal.DeviceAuthorization{DeviceCode: "provider-secret", UserCode: "ABCD-EFGH", VerificationURI: "https://github.com/login/device", ExpiresAt: now.Add(15 * time.Minute), Interval: 5 * time.Second})
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
	if _, err := manager.CompletePoll(lease, githubexternal.DevicePollResult{Status: githubexternal.DevicePollAuthorized, Tokens: tokens}, []githubexternal.InstallationCandidate{candidate}, "kingdom"); err != nil {
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
	access, err := manager.BeginFinalizeVerification(flow.ID, flow.SessionID, flow.Origin)
	if err != nil || access.AccessToken != "access-token" || !access.ExpiresAt.Equal(flow.ExpiresAt) {
		t.Fatalf("finalize access=%#v err=%v", access, err)
	}
	// The client cannot invent an installation the fresh listing does not carry.
	if _, _, err := manager.CompleteFinalizeVerification(access, []string{"999"}, []githubexternal.InstallationCandidate{candidate}); !errors.Is(err, errDeviceFlowStage) {
		t.Fatalf("spoofed candidate error=%v", err)
	}
	access, err = manager.BeginFinalizeVerification(flow.ID, flow.SessionID, flow.Origin)
	if err != nil {
		t.Fatalf("reacquire finalize verification after rejected candidate: %v", err)
	}
	finalize, selected, err := manager.CompleteFinalizeVerification(access, []string{"2"}, []githubexternal.InstallationCandidate{candidate})
	if err != nil || len(selected) != 1 || selected[0].AccountID != "10" || finalize.Tokens.AccessToken != "access-token" {
		t.Fatalf("finalize=%#v candidate=%#v err=%v", finalize, selected, err)
	}
	if _, _, err := manager.CompleteFinalizeVerification(access, []string{"2"}, []githubexternal.InstallationCandidate{candidate}); !errors.Is(err, errDeviceFlowStage) {
		t.Fatalf("replay finalize error=%v", err)
	}
}

func TestDeviceFlowFinalizationRemainsPinnedAcrossExpiry(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	manager := newDeviceFlowManager(func() time.Time { return now })
	lease := deviceFinalizeLease{deviceOperationLease: deviceOperationLease{FlowID: "flow-finalizing", Generation: "lease-finalizing", Kind: deviceOperationFinalize}}
	manager.flows["flow-finalizing"] = &githubDeviceFlow{ID: "flow-finalizing", SessionID: "session", Origin: "https://kingdom.example", Stage: deviceStageFinalizing, ExpiresAt: now.Add(time.Minute), LeaseGeneration: lease.Generation, LeaseKind: lease.Kind}

	now = now.Add(2 * time.Minute)
	if _, err := manager.Status("flow-finalizing", "session", "https://kingdom.example", "kingdom"); !errors.Is(err, errDeviceFlowNotFound) {
		t.Fatalf("expired finalizing status error=%v", err)
	}
	if err := manager.Finalize(lease, "connection"); err != nil {
		t.Fatalf("finalizing flow was pruned during durable completion: %v", err)
	}
}

func TestDeviceFlowCandidateRefreshCannotReplaceFinalizeVerificationLease(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	manager := newDeviceFlowManager(func() time.Time { return now })
	flow := &githubDeviceFlow{
		ID:        "flow-finalize-race",
		SessionID: "session",
		Origin:    "https://kingdom.example",
		Stage:     deviceStageSelecting,
		ExpiresAt: now.Add(time.Hour),
		Tokens:    githubexternal.UserTokens{AccessToken: "access-token", RefreshToken: "refresh-token"},
	}
	manager.flows[flow.ID] = flow

	if _, err := manager.BeginFinalizeVerification(flow.ID, flow.SessionID, flow.Origin); err != nil {
		t.Fatalf("acquire finalize verification lease: %v", err)
	}
	if _, err := manager.BeginCandidateRefresh(flow.ID, flow.SessionID, flow.Origin); !errors.Is(err, errDeviceFlowInFlight) {
		t.Fatalf("candidate refresh during finalize verification error=%v, want %v", err, errDeviceFlowInFlight)
	}
	if flow.Stage != deviceStageSelecting || flow.LeaseKind != deviceOperationFinalizeVerification || flow.Tokens.AccessToken != "access-token" {
		t.Fatalf("candidate refresh replaced finalize lease: %#v", flow)
	}
}

func TestDeviceFlowStalePollCompletionCannotSettleCandidateRefresh(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	manager := newDeviceFlowManager(func() time.Time { return now })
	flow := createDeviceFlow(t, manager, "session", "https://kingdom.example", "", now.Add(time.Hour), githubexternal.DeviceAuthorization{DeviceCode: "provider-secret", UserCode: "ABCD-EFGH", VerificationURI: "https://github.com/login/device", ExpiresAt: now.Add(time.Hour), Interval: 0})
	pollLease, _, err := manager.BeginPoll(flow.ID, flow.SessionID, flow.Origin)
	if err != nil {
		t.Fatal(err)
	}
	tokens := githubexternal.UserTokens{AccessToken: "access-token", RefreshToken: "refresh-token", AccessExpiresAt: now.Add(time.Hour), RefreshExpiresAt: now.Add(180 * 24 * time.Hour)}
	first := githubexternal.InstallationCandidate{InstallationID: "2", AccountID: "10", AccountLogin: "octocat", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom"}
	if _, err := manager.CompletePoll(pollLease, githubexternal.DevicePollResult{Status: githubexternal.DevicePollAuthorized, Tokens: tokens}, []githubexternal.InstallationCandidate{first}, "kingdom"); err != nil {
		t.Fatal(err)
	}
	refreshLease, err := manager.BeginCandidateRefresh(flow.ID, flow.SessionID, flow.Origin)
	if err != nil {
		t.Fatal(err)
	}
	if pollLease.Generation == refreshLease.Generation || pollLease.Kind == refreshLease.Kind {
		t.Fatalf("operations did not receive separate opaque leases: poll=%#v refresh=%#v", pollLease.deviceOperationLease, refreshLease.deviceOperationLease)
	}
	if _, err := manager.CompletePoll(pollLease, githubexternal.DevicePollResult{Status: githubexternal.DevicePollPending}, nil, "kingdom"); !errors.Is(err, errDeviceFlowStage) {
		t.Fatalf("stale poll completion error=%v, want stage error", err)
	}
	if err := manager.Fail(pollLease.deviceOperationLease, "stale poll failed"); !errors.Is(err, errDeviceFlowStage) {
		t.Fatalf("stale poll failure error=%v, want stage error", err)
	}
	retained := manager.flows[flow.ID]
	if retained.Stage != deviceStageSelecting || retained.LeaseGeneration != refreshLease.Generation || retained.LeaseKind != deviceOperationCandidateRefresh {
		t.Fatalf("stale poll settled candidate refresh: %#v", retained)
	}
	second := githubexternal.InstallationCandidate{InstallationID: "3", AccountID: "11", AccountLogin: "acme", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom"}
	status, err := manager.CompleteCandidateRefresh(refreshLease, []githubexternal.InstallationCandidate{first, second}, "kingdom")
	if err != nil || status.Stage != deviceStageSelecting || len(status.Candidates) != 2 {
		t.Fatalf("refresh completion status=%#v err=%v", status, err)
	}
}

func TestDeviceFlowStartingReservationIsCurrentAndNotDiscardableWhileOwned(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	manager := newDeviceFlowManager(func() time.Time { return now })
	flow, lease, created, err := manager.ReserveStart("session", "https://kingdom.example", "conn-reconnect", now.Add(time.Hour))
	if err != nil || !created {
		t.Fatalf("reserve start: created=%t err=%v", created, err)
	}
	status, err := manager.Current(flow.SessionID, flow.Origin, "kingdom")
	if err != nil || status.Stage != deviceStageStarting || status.FlowID != flow.ID || status.ReconnectConnectionID != "conn-reconnect" || status.IntervalSeconds != 0 || status.Candidates == nil {
		t.Fatalf("starting current status=%#v err=%v", status, err)
	}
	raw, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"provider-secret", "device_code", "user_code", "verification_uri", "access_token", "refresh_token"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("starting reservation exposed %q: %s", forbidden, raw)
		}
	}
	if err := manager.Discard(flow.ID, flow.SessionID, flow.Origin); !errors.Is(err, errDeviceFlowInFlight) {
		t.Fatalf("discard owned start error=%v", err)
	}
	manager.AbortStart(lease)
	if _, err := manager.Current(flow.SessionID, flow.Origin, "kingdom"); !errors.Is(err, errDeviceFlowNotFound) {
		t.Fatalf("aborted start remained current: %v", err)
	}
}

func TestDeviceFlowCompletionsReturnSafeStatusAtomicallyAcrossExpiry(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	authorization := githubexternal.DeviceAuthorization{DeviceCode: "provider-secret", UserCode: "ABCD-EFGH", VerificationURI: "https://github.com/login/device", ExpiresAt: now.Add(time.Minute), Interval: 0}

	t.Run("poll", func(t *testing.T) {
		manager := newDeviceFlowManager(func() time.Time { return now })
		flow := createDeviceFlow(t, manager, "session", "https://kingdom.example", "", now.Add(time.Hour), authorization)
		lease, _, err := manager.BeginPoll(flow.ID, flow.SessionID, flow.Origin)
		if err != nil {
			t.Fatal(err)
		}
		now = now.Add(2 * time.Minute)
		status, err := manager.CompletePoll(lease, githubexternal.DevicePollResult{Status: githubexternal.DevicePollPending}, nil, "kingdom")
		if err != nil || status.FlowID != flow.ID || status.Stage != deviceStageAwaitingAuthorization || status.Candidates == nil {
			t.Fatalf("atomic poll status=%#v err=%v", status, err)
		}
		if _, err := manager.Status(flow.ID, flow.SessionID, flow.Origin, "kingdom"); !errors.Is(err, errDeviceFlowNotFound) {
			t.Fatalf("expired flow remained available to a second status lookup: %v", err)
		}
	})

	t.Run("candidate refresh", func(t *testing.T) {
		now = time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
		manager := newDeviceFlowManager(func() time.Time { return now })
		flow := createDeviceFlow(t, manager, "session", "https://kingdom.example", "", now.Add(time.Hour), authorization)
		manager.flows[flow.ID].Stage = deviceStageNeedsInstallation
		manager.flows[flow.ID].Tokens = githubexternal.UserTokens{AccessToken: "access-token"}
		lease, err := manager.BeginCandidateRefresh(flow.ID, flow.SessionID, flow.Origin)
		if err != nil {
			t.Fatal(err)
		}
		now = now.Add(2 * time.Minute)
		status, err := manager.CompleteCandidateRefresh(lease, nil, "kingdom")
		if err != nil || status.FlowID != flow.ID || status.Stage != deviceStageNeedsInstallation || status.Candidates == nil {
			t.Fatalf("atomic candidate status=%#v err=%v", status, err)
		}
		if _, err := manager.Status(flow.ID, flow.SessionID, flow.Origin, "kingdom"); !errors.Is(err, errDeviceFlowNotFound) {
			t.Fatalf("expired flow remained available to a second status lookup: %v", err)
		}
	})
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
	active := createDeviceFlow(t, manager, "active", "https://kingdom.example", "", now.Add(2*time.Hour), authorization("active-secret"))

	var firstTerminalID string
	for i := 0; i < 100; i++ {
		now = now.Add(time.Second)
		flow := createDeviceFlow(t, manager, "terminal", "https://kingdom.example", "", now.Add(2*time.Hour), authorization(fmt.Sprintf("device-secret-%d", i)))
		if i%2 == 0 {
			lease := deviceOperationLease{FlowID: flow.ID, Generation: fmt.Sprintf("failure-lease-%d", i), Kind: deviceOperationPoll}
			manager.flows[flow.ID].LeaseGeneration, manager.flows[flow.ID].LeaseKind = lease.Generation, lease.Kind
			if err := manager.Fail(lease, "failed"); err != nil {
				t.Fatalf("fail flow %d: %v", i, err)
			}
		} else {
			manager.flows[flow.ID].Stage = deviceStageFinalizing
			manager.flows[flow.ID].Tokens = githubexternal.UserTokens{AccessToken: "access-secret", RefreshToken: "refresh-secret"}
			lease := deviceFinalizeLease{deviceOperationLease: deviceOperationLease{FlowID: flow.ID, Generation: fmt.Sprintf("finalize-lease-%d", i), Kind: deviceOperationFinalize}}
			manager.flows[flow.ID].LeaseGeneration, manager.flows[flow.ID].LeaseKind = lease.Generation, lease.Kind
			if finalizeErr := manager.Finalize(lease, fmt.Sprintf("connection-%d", i)); finalizeErr != nil {
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
			flow := createDeviceFlow(t, manager, "session-1", "https://kingdom.example", "", now.Add(10*time.Minute), githubexternal.DeviceAuthorization{DeviceCode: "provider-secret", UserCode: "ABCD-EFGH", VerificationURI: "https://github.com/login/device", ExpiresAt: now.Add(15 * time.Minute), Interval: 0})
			lease, _, err := manager.BeginPoll(flow.ID, flow.SessionID, flow.Origin)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := manager.CompletePoll(lease, githubexternal.DevicePollResult{Status: githubexternal.DevicePollAuthorized, Tokens: tokens}, test.candidates, "kingdom-issue-importer"); err != nil {
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
	flow := createDeviceFlow(t, manager, "session-1", "https://kingdom.example", "", now.Add(10*time.Minute), githubexternal.DeviceAuthorization{DeviceCode: "provider-secret", UserCode: "ABCD-EFGH", VerificationURI: "https://github.com/login/device", ExpiresAt: now.Add(15 * time.Minute), Interval: 0})
	pollLease, _, err := manager.BeginPoll(flow.ID, flow.SessionID, flow.Origin)
	if err != nil {
		t.Fatal(err)
	}
	tokens := githubexternal.UserTokens{AccessToken: "access-token", RefreshToken: "refresh-token", AccessExpiresAt: now.Add(time.Hour), RefreshExpiresAt: now.Add(180 * 24 * time.Hour)}
	first := githubexternal.InstallationCandidate{InstallationID: "2", DisplayName: "octocat", AccountID: "10", AccountLogin: "octocat", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom-issue-importer", RepositorySelection: "selected"}
	if _, err := manager.CompletePoll(pollLease, githubexternal.DevicePollResult{Status: githubexternal.DevicePollAuthorized, Tokens: tokens}, []githubexternal.InstallationCandidate{first}, "kingdom-issue-importer"); err != nil {
		t.Fatal(err)
	}

	// Installing on a second account mid-flow must be discoverable without repeating authorization.
	refreshLease, err := manager.BeginCandidateRefresh(flow.ID, flow.SessionID, flow.Origin)
	if err != nil {
		t.Fatalf("refresh from selecting_installation: %v", err)
	}
	second := githubexternal.InstallationCandidate{InstallationID: "3", DisplayName: "acme", AccountID: "11", AccountLogin: "acme", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom-issue-importer", RepositorySelection: "all"}
	if _, err := manager.CompleteCandidateRefresh(refreshLease, []githubexternal.InstallationCandidate{first, second}, "kingdom-issue-importer"); err != nil {
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

func TestDeviceFlowStartReservationResumesSameIntentAndConflictsWithAnother(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	manager := newDeviceFlowManager(func() time.Time { return now })
	_, startLease, created, err := manager.ReserveStart("session", "https://kingdom.example", "conn-1", now.Add(time.Hour))
	if err != nil || !created {
		t.Fatalf("first reservation created=%v err=%v", created, err)
	}
	if _, _, _, err := manager.ReserveStart("session", "https://kingdom.example", "conn-1", now.Add(time.Hour)); !errors.Is(err, errDeviceFlowInFlight) {
		t.Fatalf("duplicate in-flight start error=%v", err)
	}
	authorization := githubexternal.DeviceAuthorization{DeviceCode: "provider-secret", UserCode: "ABCD-EFGH", VerificationURI: "https://github.com/login/device", ExpiresAt: now.Add(15 * time.Minute), Interval: 5 * time.Second}
	started, err := manager.CompleteStart(startLease, authorization)
	if err != nil {
		t.Fatal(err)
	}
	resumed, _, created, err := manager.ReserveStart("session", "https://kingdom.example", "conn-1", now.Add(time.Hour))
	if err != nil || created || resumed.ID != started.ID {
		t.Fatalf("same intent did not resume: flow=%#v created=%v err=%v", resumed, created, err)
	}
	if _, _, _, err := manager.ReserveStart("session", "https://kingdom.example", "conn-2", now.Add(time.Hour)); !errors.Is(err, errDeviceFlowIntentConflict) {
		t.Fatalf("different reconnect intent error=%v", err)
	}
	if _, _, _, err := manager.ReserveStart("session", "https://kingdom.example", "", now.Add(time.Hour)); !errors.Is(err, errDeviceFlowIntentConflict) {
		t.Fatalf("fresh intent while reconnect active error=%v", err)
	}

	status, err := manager.Current("session", "https://kingdom.example", "kingdom")
	if err != nil || status.FlowID != started.ID || status.ReconnectConnectionID != "conn-1" {
		t.Fatalf("current status=%#v err=%v", status, err)
	}
	if _, err := manager.Current("other-session", "https://kingdom.example", "kingdom"); !errors.Is(err, errDeviceFlowNotFound) {
		t.Fatalf("cross-binding current error=%v", err)
	}
}

func TestDeviceFlowDiscardIsBoundAndRejectsInFlightWork(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	manager := newDeviceFlowManager(func() time.Time { return now })
	flow := createDeviceFlow(t, manager, "session", "https://kingdom.example", "", now.Add(time.Hour), githubexternal.DeviceAuthorization{DeviceCode: "provider-secret", UserCode: "ABCD-EFGH", VerificationURI: "https://github.com/login/device", ExpiresAt: now.Add(15 * time.Minute), Interval: 5 * time.Second})
	retained := manager.flows[flow.ID]
	if err := manager.Discard(flow.ID, "other-session", flow.Origin); !errors.Is(err, errDeviceFlowNotFound) {
		t.Fatalf("cross-session discard error=%v", err)
	}
	now = now.Add(5 * time.Second)
	pollLease, _, err := manager.BeginPoll(flow.ID, flow.SessionID, flow.Origin)
	if err != nil {
		t.Fatal(err)
	}
	resumed, _, created, err := manager.ReserveStart(flow.SessionID, flow.Origin, "", flow.ExpiresAt)
	if err != nil || created || resumed.ID != flow.ID {
		t.Fatalf("same intent did not resume during poll: flow=%#v created=%v err=%v", resumed, created, err)
	}
	if err := manager.Discard(flow.ID, flow.SessionID, flow.Origin); !errors.Is(err, errDeviceFlowInFlight) {
		t.Fatalf("in-flight discard error=%v", err)
	}
	if _, err := manager.CompletePoll(pollLease, githubexternal.DevicePollResult{Status: githubexternal.DevicePollPending}, nil, "kingdom"); err != nil {
		t.Fatal(err)
	}
	if err := manager.Discard(flow.ID, flow.SessionID, flow.Origin); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Current(flow.SessionID, flow.Origin, "kingdom"); !errors.Is(err, errDeviceFlowNotFound) {
		t.Fatalf("discarded flow remained current: %v", err)
	}
	if manager.flows[flow.ID] != nil || retained.DeviceCode != "" || retained.UserCode != "" {
		t.Fatalf("discard did not remove and clear initialized flow: %#v", retained)
	}
}

func TestDeviceFlowCapacityIsReservedBeforeAuthorization(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	manager := newDeviceFlowManager(func() time.Time { return now })
	manager.maxRetained = 1
	if _, _, created, err := manager.ReserveStart("session-1", "https://one.example", "", now.Add(time.Hour)); err != nil || !created {
		t.Fatalf("first reservation created=%v err=%v", created, err)
	}
	if _, _, _, err := manager.ReserveStart("session-2", "https://two.example", "", now.Add(time.Hour)); !errors.Is(err, errDeviceFlowCapacity) {
		t.Fatalf("capacity reservation error=%v", err)
	}
}

func TestReconnectFinalizationPolicyRequiresExactSavedSetAndPinnedCandidates(t *testing.T) {
	connection := state.ExternalConnection{
		ConnectionID: "conn-shared", CredentialStrategy: "user_access_token", AppID: "1", ClientID: "IvShared",
		Installations: []state.ConnectionInstallation{
			{InstallationID: "10", AccountID: "100", AccountLogin: "enkiid"},
			{InstallationID: "11", AccountID: "101", AccountLogin: "T-RexLabs"},
		},
	}
	candidate := func(id, accountID string) githubexternal.InstallationCandidate {
		return githubexternal.InstallationCandidate{InstallationID: id, AccountID: accountID, AccountLogin: "login-" + id, AppID: "1", ClientID: "IvShared", AppSlug: "kingdom"}
	}

	for _, test := range []struct {
		name        string
		requested   []string
		wantMissing []string
		wantExtra   []string
	}{
		{name: "exact", requested: []string{"11", "10"}},
		{name: "subset", requested: []string{"10"}, wantMissing: []string{"11"}},
		{name: "extra", requested: []string{"10", "11", "12"}, wantExtra: []string{"12"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			missing, extra := installationSetDifference(connection.InstallationIDs(), test.requested)
			if !slices.Equal(missing, test.wantMissing) || !slices.Equal(extra, test.wantExtra) {
				t.Fatalf("missing=%v extra=%v", missing, extra)
			}
		})
	}

	selected, missing, mismatch := reconnectCandidates(connection, []githubexternal.InstallationCandidate{candidate("10", "100"), candidate("11", "101"), candidate("12", "102")})
	if len(selected) != 2 || len(missing) != 0 || mismatch {
		t.Fatalf("exact reconnect candidates selected=%#v missing=%v mismatch=%v", selected, missing, mismatch)
	}
	_, missing, mismatch = reconnectCandidates(connection, []githubexternal.InstallationCandidate{candidate("10", "100")})
	if !slices.Equal(missing, []string{"11"}) || mismatch {
		t.Fatalf("provider subset missing=%v mismatch=%v", missing, mismatch)
	}
	_, missing, mismatch = reconnectCandidates(connection, []githubexternal.InstallationCandidate{candidate("10", "100"), candidate("11", "999")})
	if len(missing) != 0 || !mismatch {
		t.Fatalf("pinned identity change missing=%v mismatch=%v", missing, mismatch)
	}
}

func TestPublishedSharedReauthorizationReconcilesAmbiguousCompletionWithoutFailingFlow(t *testing.T) {
	handler, _ := newAPIServerForTest(t)
	server := handler.(*Server)
	at := "2026-08-24T14:00:00Z"
	connection, err := server.externalStore.FinalizeConnection(context.Background(), state.FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{ConnectionID: "conn-reconcile-ambiguous", CredentialStrategy: "user_access_token", DisplayName: "Shared", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom", Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}, CredentialHandle: "github-user:conn-reconcile-ambiguous", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-reconcile-create"}})
	if err != nil {
		t.Fatal(err)
	}
	projected, err := server.externalStore.ReauthorizeSharedConnection(context.Background(), connection, "kingdom", connection.Installations, "local-operator", "audit-reconcile-reauthorize", "2026-08-24T14:01:00Z")
	if err != nil {
		t.Fatal(err)
	}
	published, err := server.sharedReauthorizationPublished(context.Background(), connection.ConnectionID, projected.CredentialGeneration)
	if err != nil || !published {
		t.Fatalf("published=%t err=%v", published, err)
	}
	lease := deviceFinalizeLease{deviceOperationLease: deviceOperationLease{FlowID: "flow-reconcile-ambiguous", Generation: "lease-reconcile-ambiguous", Kind: deviceOperationFinalize}}
	server.deviceFlows.flows[lease.FlowID] = &githubDeviceFlow{ID: lease.FlowID, Stage: deviceStageFinalizing, ExpiresAt: time.Now().Add(time.Hour), LeaseGeneration: lease.Generation, LeaseKind: lease.Kind}
	// The mutation returned an error to its caller, but the exact target is
	// already published. The API therefore completes rather than failing the
	// flow, using the projection returned by ReauthorizeSharedConnection.
	if err := server.deviceFlows.Finalize(lease, projected.ConnectionID); err != nil {
		t.Fatal(err)
	}
	if flow := server.deviceFlows.flows[lease.FlowID]; flow.Stage != deviceStageCompleted || flow.Failure != "" {
		t.Fatalf("reconciled flow=%#v", flow)
	}
	if next, err := server.sharedReauthorizationPublished(context.Background(), connection.ConnectionID, projected.CredentialGeneration+1); err != nil || next {
		t.Fatalf("unpublished next generation reported published=%t err=%v", next, err)
	}
}

func TestDeviceFlowStatusAlwaysSerializesCandidatesAsArray(t *testing.T) {
	t.Parallel()

	for _, stage := range []deviceFlowStage{
		deviceStageAwaitingAuthorization,
		deviceStageDiscovering,
		deviceStageNeedsInstallation,
		deviceStageFinalizing,
		deviceStageCompleted,
		deviceStageDenied,
		deviceStageExpired,
		deviceStageFailed,
	} {
		stage := stage
		t.Run(string(stage), func(t *testing.T) {
			t.Parallel()
			status := statusFromDeviceFlow(&githubDeviceFlow{
				ID:        "flow-contract-123456789",
				Stage:     stage,
				ExpiresAt: time.Date(2026, 8, 24, 14, 0, 0, 0, time.UTC),
				Interval:  5 * time.Second,
			}, "kingdom")
			raw, err := json.Marshal(status)
			if err != nil {
				t.Fatalf("marshal device flow status: %v", err)
			}
			var payload map[string]json.RawMessage
			if err := json.Unmarshal(raw, &payload); err != nil {
				t.Fatalf("decode device flow status: %v", err)
			}
			if got := string(payload["candidates"]); got != "[]" {
				t.Fatalf("stage %q candidates must serialize as [], got %s", stage, got)
			}
		})
	}
}

func TestCurrentAndDiscardGitHubDeviceFlowEndpointsAreBrowserBound(t *testing.T) {
	handler, _ := newAPIServerForTest(t)
	server := handler.(*Server)
	cookie, session := seedBrowserSessionForTest(t, server, "http://127.0.0.1:9090", server.now().UTC().Add(time.Hour))
	cookieKey := sha256.Sum256([]byte(cookie.Value))
	flow := createDeviceFlow(t, server.deviceFlows, sessionID(cookieKey), session.Origin.Serialized, "conn-reconnect", session.ExpiresAt, githubexternal.DeviceAuthorization{DeviceCode: "provider-secret", UserCode: "ABCD-EFGH", VerificationURI: "https://github.com/login/device", ExpiresAt: server.now().Add(15 * time.Minute), Interval: 5 * time.Second})

	request := func(method, path, key string, withCSRF bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "http://127.0.0.1:9090/api/v1"+path, nil)
		req.RemoteAddr, req.Host = "127.0.0.1:49152", "127.0.0.1:9090"
		req.Header.Set("Origin", "http://127.0.0.1:9090")
		if key != "" {
			req.Header.Set("Idempotency-Key", testIdempotencyKey(t, key, time.Now()))
		}
		if withCSRF {
			req.Header.Set("X-CSRF-Token", session.CSRFToken)
		}
		req.AddCookie(cookie)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		return response
	}

	current := request(http.MethodGet, "/connections/github/device-flows", "", false)
	if current.Code != http.StatusOK || !strings.Contains(current.Body.String(), `"reconnect_connection_id":"conn-reconnect"`) {
		t.Fatalf("current status=%d body=%s", current.Code, current.Body.String())
	}
	assertRawEmptyDeviceFlowCandidates(t, current)
	for _, secret := range []string{"provider-secret", "device_code"} {
		if strings.Contains(current.Body.String(), secret) {
			t.Fatalf("current status leaked %q: %s", secret, current.Body.String())
		}
	}
	if response := request(http.MethodDelete, "/connections/github/device-flows/"+flow.ID, "device-discard-no-csrf", false); response.Code != http.StatusForbidden {
		t.Fatalf("discard without CSRF status=%d body=%s", response.Code, response.Body.String())
	}
	server.deviceFlows.flows[flow.ID].LeaseGeneration = "discard-in-flight"
	server.deviceFlows.flows[flow.ID].LeaseKind = deviceOperationCandidateRefresh
	if response := request(http.MethodDelete, "/connections/github/device-flows/"+flow.ID, "device-discard-in-flight", true); response.Code != http.StatusConflict {
		t.Fatalf("in-flight discard status=%d body=%s", response.Code, response.Body.String())
	}
	server.deviceFlows.flows[flow.ID].clearLease()
	if response := request(http.MethodDelete, "/connections/github/device-flows/"+flow.ID, "device-discard", true); response.Code != http.StatusNoContent {
		t.Fatalf("discard status=%d body=%s", response.Code, response.Body.String())
	}
	if response := request(http.MethodDelete, "/connections/github/device-flows/"+flow.ID, "device-discard", true); response.Code != http.StatusNoContent {
		t.Fatalf("idempotent discard replay status=%d body=%s", response.Code, response.Body.String())
	}
	if response := request(http.MethodGet, "/connections/github/device-flows", "", false); response.Code != http.StatusNoContent {
		t.Fatalf("current after discard status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestStartGitHubDeviceFlowEndpointResumesAndReservesCapacityBeforeProviderCall(t *testing.T) {
	providerCalls := 0
	var provider *httptest.Server
	provider = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/login/device/code" {
			http.NotFound(w, r)
			return
		}
		providerCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"device_code":"provider-secret","user_code":"ABCD-EFGH","verification_uri":%q,"expires_in":900,"interval":5}`, provider.URL+"/login/device")
	}))
	t.Cleanup(provider.Close)
	oauth, err := githubexternal.NewDeviceOAuthClient(githubexternal.DeviceOAuthClientConfig{APIBaseURL: provider.URL, WebBaseURL: provider.URL, AllowTestEndpoints: true})
	if err != nil {
		t.Fatal(err)
	}
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{GitHubSharedAppSlug: "kingdom", GitHubSharedAppClientID: "IvShared", GitHubDeviceOAuthClient: oauth})
	server := handler.(*Server)
	cookie, session := seedBrowserSessionForTest(t, server, "http://127.0.0.1:9090", server.now().UTC().Add(time.Hour))

	postStart := func(logicalKey string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:9090/api/v1/connections/github/device-flows", strings.NewReader(`{}`))
		req.RemoteAddr, req.Host = "127.0.0.1:49152", "127.0.0.1:9090"
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "http://127.0.0.1:9090")
		req.Header.Set("X-CSRF-Token", session.CSRFToken)
		req.Header.Set("Idempotency-Key", testIdempotencyKey(t, logicalKey, time.Now()))
		req.AddCookie(cookie)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		return response
	}

	first := postStart("device-start-first")
	if first.Code != http.StatusCreated {
		t.Fatalf("first start status=%d body=%s", first.Code, first.Body.String())
	}
	assertRawEmptyDeviceFlowCandidates(t, first)
	var started deviceFlowStatus
	if err := json.Unmarshal(first.Body.Bytes(), &started); err != nil {
		t.Fatal(err)
	}
	second := postStart("device-start-same-intent")
	if second.Code != http.StatusOK || providerCalls != 1 || !strings.Contains(second.Body.String(), started.FlowID) {
		t.Fatalf("resume status=%d provider_calls=%d body=%s", second.Code, providerCalls, second.Body.String())
	}
	assertRawEmptyDeviceFlowCandidates(t, second)

	if err := server.deviceFlows.Discard(started.FlowID, sessionID(sha256.Sum256([]byte(cookie.Value))), session.Origin.Serialized); err != nil {
		t.Fatal(err)
	}
	server.deviceFlows.maxRetained = 1
	createDeviceFlow(t, server.deviceFlows, "other-session", "https://other.example", "", server.now().Add(time.Hour), githubexternal.DeviceAuthorization{DeviceCode: "other-provider-secret", UserCode: "WXYZ-1234", VerificationURI: "https://github.com/login/device", ExpiresAt: server.now().Add(15 * time.Minute), Interval: 5 * time.Second})
	beforeCapacity := providerCalls
	capacity := postStart("device-start-capacity")
	if capacity.Code != http.StatusTooManyRequests || providerCalls != beforeCapacity {
		t.Fatalf("capacity status=%d provider_calls=%d before=%d body=%s", capacity.Code, providerCalls, beforeCapacity, capacity.Body.String())
	}
}

func TestStartGitHubDeviceFlowSurvivesRequestCancellation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	providerStarted := make(chan struct{})
	releaseProvider := make(chan struct{})
	var provider *httptest.Server
	provider = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/login/device/code" {
			http.NotFound(w, r)
			return
		}
		close(providerStarted)
		<-releaseProvider
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"device_code":"provider-secret","user_code":"ABCD-EFGH","verification_uri":%q,"expires_in":900,"interval":5}`, provider.URL+"/login/device")
	}))
	t.Cleanup(provider.Close)
	oauth, err := githubexternal.NewDeviceOAuthClient(githubexternal.DeviceOAuthClientConfig{Now: func() time.Time { return now }, APIBaseURL: provider.URL, WebBaseURL: provider.URL, AllowTestEndpoints: true})
	if err != nil {
		t.Fatal(err)
	}
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	t.Cleanup(cancelLifecycle)
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{StartupContext: lifecycleCtx, Now: func() time.Time { return now }, GitHubSharedAppSlug: "kingdom", GitHubSharedAppClientID: "IvShared", GitHubDeviceOAuthClient: oauth})
	server := handler.(*Server)
	cookie, session := seedBrowserSessionForTest(t, server, "http://127.0.0.1:9090", now.Add(time.Hour))
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:9090/api/v1/connections/github/device-flows", strings.NewReader(`{}`)).WithContext(requestCtx)
	req.RemoteAddr, req.Host = "127.0.0.1:49152", "127.0.0.1:9090"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://127.0.0.1:9090")
	req.Header.Set("X-CSRF-Token", session.CSRFToken)
	req.Header.Set("Idempotency-Key", testIdempotencyKey(t, "device-start-cancel", now))
	req.AddCookie(cookie)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, req)
		close(done)
	}()
	<-providerStarted
	currentRequest := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9090/api/v1/connections/github/device-flows", nil)
	currentRequest.RemoteAddr, currentRequest.Host = "127.0.0.1:49152", "127.0.0.1:9090"
	currentRequest.Header.Set("Origin", "http://127.0.0.1:9090")
	currentRequest.AddCookie(cookie)
	currentResponse := httptest.NewRecorder()
	handler.ServeHTTP(currentResponse, currentRequest)
	if currentResponse.Code != http.StatusOK || !strings.Contains(currentResponse.Body.String(), `"stage":"starting_authorization"`) {
		t.Fatalf("starting current status=%d body=%s", currentResponse.Code, currentResponse.Body.String())
	}
	assertRawEmptyDeviceFlowCandidates(t, currentResponse)
	for _, forbidden := range []string{"provider-secret", "device_code", "user_code", "verification_uri", "access_token", "refresh_token"} {
		if strings.Contains(currentResponse.Body.String(), forbidden) {
			t.Fatalf("starting current response exposed %q: %s", forbidden, currentResponse.Body.String())
		}
	}
	cancelRequest()
	close(releaseProvider)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("device start did not finish after request cancellation")
	}
	if response.Code != http.StatusCreated {
		t.Fatalf("start status=%d body=%s", response.Code, response.Body.String())
	}
	status, err := server.deviceFlows.Current(sessionID(sha256.Sum256([]byte(cookie.Value))), session.Origin.Serialized, "kingdom")
	if err != nil || status.Stage != deviceStageAwaitingAuthorization || status.UserCode != "ABCD-EFGH" {
		t.Fatalf("started flow status=%#v err=%v", status, err)
	}
}

func TestGitHubDevicePollAndRefreshSurviveRequestCancellation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	for _, operation := range []string{"poll", "refresh"} {
		t.Run(operation, func(t *testing.T) {
			providerStarted := make(chan struct{})
			releaseProvider := make(chan struct{})
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/login/oauth/access_token":
					close(providerStarted)
					<-releaseProvider
					_, _ = w.Write([]byte(`{"error":"authorization_pending"}`))
				case "/user":
					close(providerStarted)
					<-releaseProvider
					_, _ = w.Write([]byte(`{"id":10,"login":"octocat"}`))
				case "/user/installations":
					_, _ = w.Write([]byte(`{"total_count":0,"installations":[]}`))
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(provider.Close)
			oauth, err := githubexternal.NewDeviceOAuthClient(githubexternal.DeviceOAuthClientConfig{Now: func() time.Time { return now }, APIBaseURL: provider.URL, WebBaseURL: provider.URL, AllowTestEndpoints: true})
			if err != nil {
				t.Fatal(err)
			}
			handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{Now: func() time.Time { return now }, GitHubSharedAppSlug: "kingdom", GitHubSharedAppClientID: "IvShared", GitHubDeviceOAuthClient: oauth})
			server := handler.(*Server)
			cookie, session := seedBrowserSessionForTest(t, server, "http://127.0.0.1:9090", now.Add(time.Hour))
			flow := createDeviceFlow(t, server.deviceFlows, sessionID(sha256.Sum256([]byte(cookie.Value))), session.Origin.Serialized, "", session.ExpiresAt, githubexternal.DeviceAuthorization{DeviceCode: "provider-secret", UserCode: "ABCD-EFGH", VerificationURI: "https://github.com/login/device", ExpiresAt: now.Add(15 * time.Minute), Interval: 0})
			path := "/connections/github/device-flows/" + flow.ID + "/poll"
			if operation == "refresh" {
				path = "/connections/github/device-flows/" + flow.ID + "/candidates"
				server.deviceFlows.flows[flow.ID].Stage = deviceStageNeedsInstallation
				server.deviceFlows.flows[flow.ID].Tokens = githubexternal.UserTokens{AccessToken: "access-token", RefreshToken: "refresh-token", AccessExpiresAt: now.Add(time.Hour), RefreshExpiresAt: now.Add(180 * 24 * time.Hour)}
			}
			requestCtx, cancelRequest := context.WithCancel(context.Background())
			req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:9090/api/v1"+path, nil).WithContext(requestCtx)
			req.RemoteAddr, req.Host = "127.0.0.1:49152", "127.0.0.1:9090"
			req.Header.Set("Origin", "http://127.0.0.1:9090")
			req.Header.Set("X-CSRF-Token", session.CSRFToken)
			req.Header.Set("Idempotency-Key", testIdempotencyKey(t, "device-"+operation+"-cancel", now))
			req.AddCookie(cookie)
			response := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				handler.ServeHTTP(response, req)
				close(done)
			}()
			<-providerStarted
			cancelRequest()
			close(releaseProvider)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("%s did not finish after request cancellation", operation)
			}
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"stage":"`+map[string]string{"poll": "awaiting_authorization", "refresh": "needs_installation"}[operation]+`"`) {
				t.Fatalf("%s status=%d body=%s", operation, response.Code, response.Body.String())
			}
		})
	}
}

func TestFinalizeGitHubDeviceFlowSurvivesRequestCancellation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	providerStarted := make(chan struct{})
	releaseProvider := make(chan struct{})
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/user":
			close(providerStarted)
			<-releaseProvider
			_, _ = w.Write([]byte(`{"id":7,"login":"authorizer"}`))
		case "/user/installations":
			_, _ = w.Write([]byte(`{"total_count":1,"installations":[{"id":2,"app_id":1,"client_id":"IvShared","app_slug":"kingdom","target_id":10,"account":{"id":10,"login":"octocat"},"permissions":{"metadata":"read","issues":"read"},"events":[],"repository_selection":"selected"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(provider.Close)
	oauth, err := githubexternal.NewDeviceOAuthClient(githubexternal.DeviceOAuthClientConfig{Now: func() time.Time { return now }, APIBaseURL: provider.URL, WebBaseURL: provider.URL, AllowTestEndpoints: true})
	if err != nil {
		t.Fatal(err)
	}
	tokenStore, err := githubexternal.NewUserTokenStore(filepath.Join(t.TempDir(), "secrets"))
	if err != nil {
		t.Fatal(err)
	}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{
		Now:                     func() time.Time { return now },
		IDGenerator:             func(prefix string) string { return prefix + "-cancel-finalize" },
		GitHubSharedAppSlug:     "kingdom",
		GitHubSharedAppClientID: "IvShared",
		GitHubDeviceOAuthClient: oauth,
		GitHubUserTokenStore:    tokenStore,
	})
	server := handler.(*Server)
	cookie, session := seedBrowserSessionForTest(t, server, "http://127.0.0.1:9090", now.Add(time.Hour))
	flow := &githubDeviceFlow{
		ID:        "flow-request-cancel-finalize",
		SessionID: sessionID(sha256.Sum256([]byte(cookie.Value))),
		Origin:    session.Origin.Serialized,
		Stage:     deviceStageSelecting,
		ExpiresAt: session.ExpiresAt,
		Tokens: githubexternal.UserTokens{
			AccessToken:      "access-token",
			RefreshToken:     "refresh-token",
			AccessExpiresAt:  now.Add(time.Hour),
			RefreshExpiresAt: now.Add(180 * 24 * time.Hour),
		},
		Candidates: []githubexternal.InstallationCandidate{{InstallationID: "2", DisplayName: "octocat", AccountID: "10", AccountLogin: "octocat", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom"}},
	}
	server.deviceFlows.flows[flow.ID] = flow
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:9090/api/v1/connections/github/device-flows/"+flow.ID+"/finalize", strings.NewReader(`{"installation_ids":["2"],"display_name":"octocat"}`)).WithContext(requestCtx)
	req.RemoteAddr, req.Host = "127.0.0.1:49152", "127.0.0.1:9090"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://127.0.0.1:9090")
	req.Header.Set("X-CSRF-Token", session.CSRFToken)
	req.Header.Set("Idempotency-Key", testIdempotencyKey(t, "device-finalize-cancel", now))
	req.AddCookie(cookie)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, req)
		close(done)
	}()
	<-providerStarted
	cancelRequest()
	close(releaseProvider)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("device finalization did not finish after request cancellation")
	}
	if response.Code != http.StatusCreated {
		t.Fatalf("finalize status=%d body=%s", response.Code, response.Body.String())
	}
	connection, err := state.NewExternalStore(db).GetConnection(context.Background(), "connection-cancel-finalize")
	if err != nil || !connection.HasCurrentValidation() || connection.CredentialGeneration != 1 {
		t.Fatalf("durable connection=%#v err=%v", connection, err)
	}
	status, err := server.deviceFlows.Status(flow.ID, flow.SessionID, flow.Origin, "kingdom")
	if err != nil || status.Stage != deviceStageCompleted || status.ConnectionID != connection.ConnectionID {
		t.Fatalf("final flow status=%#v err=%v", status, err)
	}
}

func TestPollAndRefreshGitHubDeviceFlowEndpointsSerializeEmptyCandidatesAsArray(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	authorized := false
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/login/oauth/access_token":
			if !authorized {
				_, _ = w.Write([]byte(`{"error":"authorization_pending"}`))
				return
			}
			_, _ = w.Write([]byte(`{"access_token":"access-token","refresh_token":"refresh-token","token_type":"bearer","scope":"","expires_in":3600,"refresh_token_expires_in":10000}`))
		case "/user":
			_, _ = w.Write([]byte(`{"id":10,"login":"octocat"}`))
		case "/user/installations":
			_, _ = w.Write([]byte(`{"total_count":0,"installations":[]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(provider.Close)
	oauth, err := githubexternal.NewDeviceOAuthClient(githubexternal.DeviceOAuthClientConfig{Now: func() time.Time { return now }, APIBaseURL: provider.URL, WebBaseURL: provider.URL, AllowTestEndpoints: true})
	if err != nil {
		t.Fatal(err)
	}
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{Now: func() time.Time { return now }, GitHubSharedAppSlug: "kingdom", GitHubSharedAppClientID: "IvShared", GitHubDeviceOAuthClient: oauth})
	server := handler.(*Server)
	cookie, session := seedBrowserSessionForTest(t, server, "http://127.0.0.1:9090", now.Add(time.Hour))
	cookieKey := sha256.Sum256([]byte(cookie.Value))
	flow := createDeviceFlow(t, server.deviceFlows, sessionID(cookieKey), session.Origin.Serialized, "", session.ExpiresAt, githubexternal.DeviceAuthorization{DeviceCode: "provider-secret", UserCode: "ABCD-EFGH", VerificationURI: "https://github.com/login/device", ExpiresAt: now.Add(15 * time.Minute), Interval: 5 * time.Second})

	post := func(path, logicalKey string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:9090/api/v1"+path, nil)
		req.RemoteAddr, req.Host = "127.0.0.1:49152", "127.0.0.1:9090"
		req.Header.Set("Origin", "http://127.0.0.1:9090")
		req.Header.Set("X-CSRF-Token", session.CSRFToken)
		req.Header.Set("Idempotency-Key", testIdempotencyKey(t, logicalKey, now))
		req.AddCookie(cookie)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		return response
	}

	now = now.Add(5 * time.Second)
	pending := post("/connections/github/device-flows/"+flow.ID+"/poll", "device-poll-pending")
	if pending.Code != http.StatusOK || !strings.Contains(pending.Body.String(), `"stage":"awaiting_authorization"`) {
		t.Fatalf("pending poll status=%d body=%s", pending.Code, pending.Body.String())
	}
	assertRawEmptyDeviceFlowCandidates(t, pending)

	now = now.Add(5 * time.Second)
	authorized = true
	completedPoll := post("/connections/github/device-flows/"+flow.ID+"/poll", "device-poll-authorized")
	if completedPoll.Code != http.StatusOK || !strings.Contains(completedPoll.Body.String(), `"stage":"needs_installation"`) {
		t.Fatalf("authorized poll status=%d body=%s", completedPoll.Code, completedPoll.Body.String())
	}
	assertRawEmptyDeviceFlowCandidates(t, completedPoll)

	refreshed := post("/connections/github/device-flows/"+flow.ID+"/candidates", "device-candidates-empty")
	if refreshed.Code != http.StatusOK || !strings.Contains(refreshed.Body.String(), `"stage":"needs_installation"`) {
		t.Fatalf("candidate refresh status=%d body=%s", refreshed.Code, refreshed.Body.String())
	}
	assertRawEmptyDeviceFlowCandidates(t, refreshed)
}

func TestCandidateRefreshEndpointCannotRaceFinalizeVerification(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	verificationStarted := make(chan struct{})
	releaseVerification := make(chan struct{})
	defer func() {
		select {
		case <-releaseVerification:
		default:
			close(releaseVerification)
		}
	}()
	var userCalls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/user":
			if userCalls.Add(1) == 1 {
				close(verificationStarted)
				<-releaseVerification
			}
			_, _ = w.Write([]byte(`{"id":7,"login":"authorizer"}`))
		case "/user/installations":
			_, _ = w.Write([]byte(`{"total_count":1,"installations":[{"id":2,"app_id":1,"client_id":"IvShared","app_slug":"kingdom","target_id":10,"account":{"id":10,"login":"octocat"},"permissions":{"metadata":"read","issues":"read"},"events":[],"repository_selection":"selected"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(provider.Close)
	oauth, err := githubexternal.NewDeviceOAuthClient(githubexternal.DeviceOAuthClientConfig{Now: func() time.Time { return now }, APIBaseURL: provider.URL, WebBaseURL: provider.URL, AllowTestEndpoints: true})
	if err != nil {
		t.Fatal(err)
	}
	tokenStore, err := githubexternal.NewUserTokenStore(filepath.Join(t.TempDir(), "secrets"))
	if err != nil {
		t.Fatal(err)
	}
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{
		Now:                     func() time.Time { return now },
		IDGenerator:             func(prefix string) string { return prefix + "-finalize-race" },
		GitHubSharedAppSlug:     "kingdom",
		GitHubSharedAppClientID: "IvShared",
		GitHubDeviceOAuthClient: oauth,
		GitHubUserTokenStore:    tokenStore,
	})
	server := handler.(*Server)
	cookie, session := seedBrowserSessionForTest(t, server, "http://127.0.0.1:9090", now.Add(time.Hour))
	flow := &githubDeviceFlow{
		ID:        "flow-finalize-race-endpoint",
		SessionID: sessionID(sha256.Sum256([]byte(cookie.Value))),
		Origin:    session.Origin.Serialized,
		Stage:     deviceStageSelecting,
		ExpiresAt: session.ExpiresAt,
		Tokens: githubexternal.UserTokens{
			AccessToken:      "access-token",
			RefreshToken:     "refresh-token",
			AccessExpiresAt:  now.Add(time.Hour),
			RefreshExpiresAt: now.Add(180 * 24 * time.Hour),
		},
		Candidates: []githubexternal.InstallationCandidate{{InstallationID: "2", DisplayName: "octocat", AccountID: "10", AccountLogin: "octocat", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom"}},
	}
	server.deviceFlows.flows[flow.ID] = flow

	request := func(path, body, key string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:9090/api/v1"+path, strings.NewReader(body))
		req.RemoteAddr, req.Host = "127.0.0.1:49152", "127.0.0.1:9090"
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "http://127.0.0.1:9090")
		req.Header.Set("X-CSRF-Token", session.CSRFToken)
		req.Header.Set("Idempotency-Key", key)
		req.AddCookie(cookie)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		return response
	}
	finalizeKey := testIdempotencyKey(t, "device-finalize-race", now)
	refreshKey := testIdempotencyKey(t, "device-refresh-race", now)
	finalized := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		finalized <- request("/connections/github/device-flows/"+flow.ID+"/finalize", `{"installation_ids":["2"],"display_name":"octocat"}`, finalizeKey)
	}()
	select {
	case <-verificationStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("finalize verification did not reach the provider")
	}

	refreshed := request("/connections/github/device-flows/"+flow.ID+"/candidates", "", refreshKey)
	if refreshed.Code != http.StatusConflict || userCalls.Load() != 1 {
		t.Fatalf("racing refresh status=%d provider_user_calls=%d body=%s", refreshed.Code, userCalls.Load(), refreshed.Body.String())
	}
	server.deviceFlows.mu.Lock()
	stage, leaseKind := flow.Stage, flow.LeaseKind
	server.deviceFlows.mu.Unlock()
	if stage != deviceStageSelecting || leaseKind != deviceOperationFinalizeVerification {
		t.Fatalf("racing refresh replaced finalize lease: stage=%q lease_kind=%q", stage, leaseKind)
	}

	close(releaseVerification)
	select {
	case response := <-finalized:
		if response.Code != http.StatusCreated {
			t.Fatalf("finalize status=%d body=%s", response.Code, response.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("finalize did not complete after releasing provider verification")
	}
}
