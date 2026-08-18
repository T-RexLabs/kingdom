package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/external"
	githubexternal "kingdom/internal/external/github"
	"kingdom/internal/state"
)

var githubOrganizationPattern = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$`)

type startManifestFlowRequest struct {
	OwnerType    string `json:"owner_type"`
	Organization string `json:"organization,omitempty"`
}

type startManifestFlowResponse struct {
	FlowID     string    `json:"flow_id"`
	FormAction string    `json:"form_action"`
	Manifest   string    `json:"manifest"`
	ExpiresAt  time.Time `json:"expires_at"`
}

func (s *Server) handleStartGitHubManifestFlow(w http.ResponseWriter, r *http.Request) {
	if s.githubKeys == nil {
		writeCanonicalError(w, http.StatusServiceUnavailable, errorcatalog.CodeDependencyBlocked, "GitHub private-key storage is unavailable", "api.github_manifest.start", nil)
		return
	}
	sessionID, origin, ok := s.manifestBrowserBinding(w, r, true)
	if !ok {
		return
	}
	var input startManifestFlowRequest
	if err := decodeStrictJSON(r, &input); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	input.OwnerType = strings.TrimSpace(input.OwnerType)
	input.Organization = strings.TrimSpace(input.Organization)
	if input.OwnerType != "personal" && input.OwnerType != "organization" {
		s.writeBadRequest(w, "owner_type must be personal or organization")
		return
	}
	if input.OwnerType == "personal" && input.Organization != "" {
		s.writeBadRequest(w, "organization must be omitted for a personal owner")
		return
	}
	if input.OwnerType == "organization" && !githubOrganizationPattern.MatchString(input.Organization) {
		s.writeBadRequest(w, "organization is invalid")
		return
	}
	principal, _ := authenticatedPrincipal(r.Context())
	flow, err := s.manifestFlows.Create(sessionID, origin, principal.ExpiresAt)
	if err != nil {
		if errors.Is(err, errManifestFlowCapacity) {
			writeCanonicalError(w, http.StatusTooManyRequests, errorcatalog.CodeDependencyBlocked, "too many GitHub setup flows are active; finish or wait for an existing flow to expire", "api.github_manifest.start", nil)
			return
		}
		s.writeInternalError(w, "create GitHub setup flow failed")
		return
	}
	manifestCallback := origin + s.basePath + "/connections/github/manifest-flows/manifest-callback"
	setupCallback := origin + s.basePath + "/connections/github/manifest-flows/setup-callback?state=" + url.QueryEscape(flow.SetupState)
	oauthCallback := origin + s.basePath + "/connections/github/manifest-flows/oauth-callback"
	manifest := map[string]any{
		"name":                     "Kingdom Issues",
		"url":                      origin,
		"hook_attributes":          map[string]any{"url": origin + s.basePath + "/connections/github/manifest-flows/webhook-disabled", "active": false},
		"redirect_url":             manifestCallback,
		"callback_urls":            []string{oauthCallback},
		"setup_url":                setupCallback,
		"public":                   false,
		"default_permissions":      map[string]string{"metadata": "read", "issues": "read"},
		"default_events":           []string{},
		"request_oauth_on_install": false,
		"setup_on_update":          false,
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		s.manifestFlows.Fail(flow.ID, "GitHub setup could not be started. Start a new setup flow.")
		s.writeInternalError(w, "encode GitHub App manifest failed")
		return
	}
	action := githubexternal.GitHubWebOrigin + "/settings/apps/new?state=" + url.QueryEscape(flow.ManifestState)
	if input.OwnerType == "organization" {
		action = githubexternal.GitHubWebOrigin + "/organizations/" + url.PathEscape(input.Organization) + "/settings/apps/new?state=" + url.QueryEscape(flow.ManifestState)
	}
	setSensitiveIdempotencyExpiry(w, flow.ExpiresAt)
	writeJSON(w, http.StatusCreated, startManifestFlowResponse{FlowID: flow.ID, FormAction: action, Manifest: string(raw), ExpiresAt: flow.ExpiresAt})
}

func (s *Server) handleCurrentGitHubManifestFlow(w http.ResponseWriter, r *http.Request) {
	sessionID, origin, ok := s.manifestBrowserBinding(w, r, false)
	if !ok {
		return
	}
	status, err := s.manifestFlows.Current(sessionID, origin)
	if err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleGetGitHubManifestFlow(w http.ResponseWriter, r *http.Request) {
	sessionID, origin, ok := s.manifestBrowserBinding(w, r, false)
	if !ok {
		return
	}
	status, err := s.manifestFlows.Status(r.PathValue("id"), sessionID, origin)
	if err != nil {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "GitHub setup flow is unavailable", "api.github_manifest.status", nil)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleConvertGitHubManifestFlow(w http.ResponseWriter, r *http.Request) {
	sessionID, origin, ok := s.manifestBrowserBinding(w, r, false)
	if !ok {
		return
	}
	flowID := r.PathValue("id")
	lease, err := s.manifestFlows.BeginConversion(flowID, sessionID, origin)
	if err != nil {
		writeManifestStageError(w, "GitHub App creation has not returned to Kingdom")
		return
	}
	defer zeroSecret(lease.Code)
	conversionCtx, cancel := contextWithFlowDeadline(r.Context(), s.now, lease.ExpiresAt)
	defer cancel()
	converted, err := s.githubSetup.ConvertManifest(conversionCtx, string(lease.Code))
	if err != nil {
		s.manifestFlows.Fail(flowID, "GitHub did not return the exact App configuration Kingdom requires. Start a new setup flow.")
		writeGitHubSetupError(w, "GitHub App conversion failed", err)
		return
	}
	if err := s.manifestFlows.CompleteConversion(flowID, converted); err != nil {
		s.manifestFlows.Fail(flowID, "GitHub setup expired before conversion completed. Start a new setup flow.")
		writeManifestStageError(w, "GitHub setup flow expired")
		return
	}
	installURL, err := s.manifestFlows.InstallationURL(flowID, sessionID, origin)
	if err != nil {
		s.manifestFlows.Fail(flowID, "GitHub installation could not be started. Start a new setup flow.")
		writeManifestStageError(w, "GitHub installation is unavailable")
		return
	}
	setSensitiveIdempotencyExpiry(w, lease.ExpiresAt)
	writeJSON(w, http.StatusOK, map[string]string{"installation_url": installURL})
}

func (s *Server) handleGetGitHubManifestInstallationURL(w http.ResponseWriter, r *http.Request) {
	sessionID, origin, ok := s.manifestBrowserBinding(w, r, false)
	if !ok {
		return
	}
	installURL, err := s.manifestFlows.InstallationURL(r.PathValue("id"), sessionID, origin)
	if err != nil {
		writeManifestStageError(w, "GitHub installation is not ready")
		return
	}
	status, err := s.manifestFlows.Status(r.PathValue("id"), sessionID, origin)
	if err != nil {
		writeManifestStageError(w, "GitHub installation is not ready")
		return
	}
	setSensitiveIdempotencyExpiry(w, status.ExpiresAt)
	writeJSON(w, http.StatusOK, map[string]string{"installation_url": installURL})
}

func (s *Server) handleAuthorizeGitHubManifestFlow(w http.ResponseWriter, r *http.Request) {
	sessionID, origin, ok := s.manifestBrowserBinding(w, r, false)
	if !ok {
		return
	}
	oauth, err := s.manifestFlows.OAuthRequest(r.PathValue("id"), sessionID, origin)
	if err != nil {
		writeManifestStageError(w, "GitHub installation has not returned to Kingdom")
		return
	}
	setSensitiveIdempotencyExpiry(w, oauth.ExpiresAt)
	callback := origin + s.basePath + "/connections/github/manifest-flows/oauth-callback"
	query := url.Values{"client_id": {oauth.ClientID}, "redirect_uri": {callback}, "state": {oauth.State}, "code_challenge": {oauth.Challenge}, "code_challenge_method": {"S256"}}
	writeJSON(w, http.StatusOK, map[string]string{"authorization_url": githubexternal.GitHubWebOrigin + "/login/oauth/authorize?" + query.Encode()})
}

func (s *Server) handleCompleteGitHubManifestFlow(w http.ResponseWriter, r *http.Request) {
	sessionID, origin, ok := s.manifestBrowserBinding(w, r, false)
	if !ok {
		return
	}
	flowID := r.PathValue("id")
	completion, err := s.manifestFlows.BeginCompletion(flowID, sessionID, origin)
	if err != nil {
		writeManifestStageError(w, "GitHub authorization has not returned to Kingdom")
		return
	}
	setSensitiveIdempotencyExpiry(w, completion.ExpiresAt)
	defer func() {
		zeroSecret(completion.PrivateKey)
		zeroSecret(completion.ClientSecret)
		zeroSecret(completion.OAuthCode)
		zeroSecret(completion.OAuthVerifier)
	}()
	completionCtx, cancel := contextWithFlowDeadline(r.Context(), s.now, completion.ExpiresAt)
	defer cancel()
	callback := origin + s.basePath + "/connections/github/manifest-flows/oauth-callback"
	userToken, err := s.githubSetup.ExchangeOAuthCode(completionCtx, completion.ClientID, completion.ClientSecret, completion.OAuthCode, completion.OAuthVerifier, callback)
	if err != nil {
		s.failManifestCompletion(flowID, w, "GitHub authorization could not be verified", err)
		return
	}
	defer zeroSecret(userToken)
	expected := githubexternal.SetupInstallation{InstallationID: completion.InstallationID, AppID: completion.AppID, ClientID: completion.ClientID, AppSlug: completion.AppSlug}
	proved, err := s.githubSetup.ProveUserInstallation(completionCtx, userToken, expected)
	if err != nil {
		s.failManifestCompletion(flowID, w, "The claimed installation does not belong to the authorized user and App", err)
		return
	}
	if err := s.githubSetup.ValidatePrivateKeyInstallation(completionCtx, completion.PrivateKey, proved); err != nil {
		s.failManifestCompletion(flowID, w, "GitHub App and installation validation failed", err)
		return
	}
	connectionID := s.idGenerator("connection")
	at := s.now().UTC().Format(time.RFC3339Nano)
	var connection state.ExternalConnection
	_, err = s.githubKeys.InstallAudited(connectionID, completion.PrivateKey, false, func() error {
		if err := completionCtx.Err(); err != nil {
			return err
		}
		var finalizeErr error
		connection, finalizeErr = s.externalStore.FinalizeConnection(completionCtx, state.FinalizeExternalConnectionRequest{
			CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{ConnectionID: connectionID, CredentialStrategy: "app_private_key", DisplayName: completion.AppName, AppID: completion.AppID, ClientID: completion.ClientID, AppSlug: completion.AppSlug, InstallationID: completion.InstallationID, CredentialHandle: githubexternal.CredentialHandle(connectionID), ActorID: authenticatedActorID(r.Context()), CreatedAt: at, AuditEventID: s.idGenerator("external_audit")},
			InstallationAccountID:           proved.AccountID, InstallationAccountLogin: proved.AccountLogin,
		})
		return finalizeErr
	})
	if err != nil {
		s.manifestFlows.Fail(flowID, "Kingdom could not save the validated connection. Start a new setup flow.")
		if isUniqueConstraintError(err) {
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, "this GitHub installation is already connected", "api.github_manifest.complete", nil)
			return
		}
		s.writeInternalError(w, "finalize GitHub connection failed")
		return
	}
	if err := s.manifestFlows.Finalize(flowID, connectionID, proved.AccountID, proved.AccountLogin); err != nil {
		s.writeInternalError(w, "consume GitHub setup flow failed")
		return
	}
	writeJSON(w, http.StatusCreated, presentConnection(connection, s.githubKeys, s.githubUserTokens))
}

func (s *Server) handleGitHubManifestCallback(w http.ResponseWriter, r *http.Request) {
	s.callbackHeaders(w)
	flow, err := s.manifestFlows.RecordManifestCode(r.URL.Query().Get("state"), r.URL.Query().Get("code"))
	if err != nil {
		writeCanonicalError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "GitHub setup callback is invalid, expired, or already used", "api.github_manifest.callback", nil)
		return
	}
	http.Redirect(w, r, flow.Origin+"/connections", http.StatusSeeOther)
}

func (s *Server) handleGitHubSetupCallback(w http.ResponseWriter, r *http.Request) {
	s.callbackHeaders(w)
	flow, err := s.manifestFlows.RecordInstallation(r.URL.Query().Get("state"), r.URL.Query().Get("installation_id"), r.URL.Query().Get("setup_action"))
	if err != nil {
		writeCanonicalError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "GitHub installation callback is invalid, expired, or already used", "api.github_manifest.setup_callback", nil)
		return
	}
	http.Redirect(w, r, flow.Origin+"/connections", http.StatusSeeOther)
}

func (s *Server) handleGitHubOAuthCallback(w http.ResponseWriter, r *http.Request) {
	s.callbackHeaders(w)
	flow, err := s.manifestFlows.RecordOAuthCode(r.URL.Query().Get("state"), r.URL.Query().Get("code"))
	if err != nil {
		writeCanonicalError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "GitHub authorization callback is invalid, expired, or already used", "api.github_manifest.oauth_callback", nil)
		return
	}
	http.Redirect(w, r, flow.Origin+"/connections", http.StatusSeeOther)
}

func (s *Server) callbackHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
}

func (s *Server) manifestBrowserBinding(w http.ResponseWriter, r *http.Request, requireOriginHeader bool) (string, string, bool) {
	principal, ok := authenticatedPrincipal(r.Context())
	if !ok || principal.Method != "cookie" {
		writeCanonicalError(w, http.StatusForbidden, errorcatalog.CodePolicyViolation, "GitHub setup requires a local browser session", "api.github_manifest.binding", nil)
		return "", "", false
	}
	if principal.SessionID == "" || principal.SessionOrigin == "" {
		s.writeUnauthorized(w)
		return "", "", false
	}
	if requireOriginHeader {
		origin, err := parseOriginHeader(r.Header.Values("Origin"))
		if err != nil || origin.Serialized != principal.SessionOrigin {
			writeCanonicalError(w, http.StatusForbidden, errorcatalog.CodePolicyViolation, "GitHub setup requires the browser session origin", "api.github_manifest.origin", nil)
			return "", "", false
		}
	}
	return principal.SessionID, principal.SessionOrigin, true
}

func writeManifestStageError(w http.ResponseWriter, message string) {
	writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, message, "api.github_manifest.stage", nil)
}

func writeGitHubSetupError(w http.ResponseWriter, message string, err error) {
	status := http.StatusBadGateway
	var retryAt *time.Time
	var details map[string]any
	var providerErr *external.ProviderError
	if errors.As(err, &providerErr) {
		retryAt = providerErr.RetryAt
		details = map[string]any{"provider_failure_class": string(providerErr.Class)}
		switch providerErr.Class {
		case external.FailureAuthentication:
			status = http.StatusFailedDependency
			message = "GitHub authorization is invalid or revoked; restart or reconnect GitHub setup"
		case external.FailureRateLimited:
			status = http.StatusTooManyRequests
		case external.FailureTimeout:
			status = http.StatusGatewayTimeout
		case external.FailureUnavailable:
			status = http.StatusBadGateway
		}
	}
	writeCanonicalErrorWithRetryAt(w, status, errorcatalog.CodeDependencyBlocked, message, "api.github_manifest.github", details, retryAt)
}

func contextWithFlowDeadline(parent context.Context, now func() time.Time, expiresAt time.Time) (context.Context, context.CancelFunc) {
	remaining := expiresAt.Sub(now().UTC())
	if remaining <= 0 {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, func() {}
	}
	return context.WithTimeout(parent, remaining)
}

func (s *Server) failManifestCompletion(flowID string, w http.ResponseWriter, message string, err error) {
	s.manifestFlows.Fail(flowID, message+". Start a new setup flow.")
	writeGitHubSetupError(w, message, err)
}
