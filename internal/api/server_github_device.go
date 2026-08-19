package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/external"
	githubexternal "kingdom/internal/external/github"
	"kingdom/internal/state"
)

type startDeviceFlowRequest struct {
	ConnectionID string `json:"connection_id,omitempty"`
}

type finalizeDeviceFlowRequest struct {
	InstallationID string `json:"installation_id"`
	DisplayName    string `json:"display_name"`
}

func (s *Server) handleGitHubSetupOptions(w http.ResponseWriter, _ *http.Request) {
	enabled := s.githubSharedAppSlug != "" && s.githubSharedAppClientID != "" && s.githubDeviceOAuth != nil && s.githubUserTokens != nil
	response := map[string]any{"shared_enabled": enabled}
	if enabled {
		response["app_slug"] = s.githubSharedAppSlug
		response["install_url"] = githubexternal.GitHubWebOrigin + "/apps/" + s.githubSharedAppSlug + "/installations/new"
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleStartGitHubDeviceFlow(w http.ResponseWriter, r *http.Request) {
	if s.githubSharedAppSlug == "" || s.githubDeviceOAuth == nil || s.githubUserTokens == nil {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "shared GitHub setup is not enabled", "api.github_device.start", nil)
		return
	}
	sessionID, origin, ok := s.manifestBrowserBinding(w, r, true)
	if !ok {
		return
	}
	var input startDeviceFlowRequest
	if err := decodeStrictJSON(r, &input); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	input.ConnectionID = strings.TrimSpace(input.ConnectionID)
	if input.ConnectionID != "" {
		connection, err := s.externalStore.GetConnection(r.Context(), input.ConnectionID)
		if err != nil || connection.CredentialStrategy != "user_access_token" || connection.Lifecycle == "removed" || connection.Lifecycle == "removal_pending" {
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "only a saved shared GitHub connection can be reconnected", "api.github_device.start", nil)
			return
		}
	}
	authorization, err := s.githubDeviceOAuth.Start(r.Context(), s.githubSharedAppClientID)
	if err != nil {
		writeGitHubDeviceError(w, "GitHub authorization could not be started", err)
		return
	}
	principal, _ := authenticatedPrincipal(r.Context())
	flow, err := s.deviceFlows.Create(sessionID, origin, input.ConnectionID, principal.ExpiresAt, authorization)
	if err != nil {
		if errors.Is(err, errDeviceFlowCapacity) {
			writeCanonicalError(w, http.StatusTooManyRequests, errorcatalog.CodeDependencyBlocked, "too many GitHub connection flows are active", "api.github_device.start", nil)
			return
		}
		s.writeInternalError(w, "create GitHub device flow failed")
		return
	}
	setSensitiveIdempotencyExpiry(w, flow.ExpiresAt)
	writeJSON(w, http.StatusCreated, statusFromDeviceFlow(flow, s.githubSharedAppSlug))
}

func (s *Server) handleGetGitHubDeviceFlow(w http.ResponseWriter, r *http.Request) {
	sessionID, origin, ok := s.manifestBrowserBinding(w, r, false)
	if !ok {
		return
	}
	status, err := s.deviceFlows.Status(r.PathValue("id"), sessionID, origin, s.githubSharedAppSlug)
	if err != nil {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "GitHub connection flow is unavailable", "api.github_device.status", nil)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handlePollGitHubDeviceFlow(w http.ResponseWriter, r *http.Request) {
	sessionID, origin, ok := s.manifestBrowserBinding(w, r, false)
	if !ok {
		return
	}
	lease, retryAt, err := s.deviceFlows.BeginPoll(r.PathValue("id"), sessionID, origin)
	if errors.Is(err, errDeviceFlowPaced) {
		writeCanonicalErrorWithRetryAt(w, http.StatusTooManyRequests, errorcatalog.CodeDependencyBlocked, "GitHub authorization poll is not due yet", "api.github_device.poll", nil, retryAt)
		return
	}
	if err != nil {
		writeDeviceStageError(w, "GitHub authorization cannot be polled in its current state")
		return
	}
	setSensitiveIdempotencyExpiry(w, lease.ExpiresAt)
	pollCtx, cancel := contextWithFlowDeadline(r.Context(), s.now, lease.ExpiresAt)
	defer cancel()
	result, err := s.githubDeviceOAuth.Poll(pollCtx, s.githubSharedAppClientID, lease.DeviceCode)
	if err != nil {
		s.deviceFlows.Fail(lease.FlowID, "GitHub authorization could not be completed. Start again.")
		writeGitHubDeviceError(w, "GitHub authorization failed", err)
		return
	}
	var candidates []githubexternal.InstallationCandidate
	if result.Status == githubexternal.DevicePollAuthorized {
		candidates, err = s.githubDeviceOAuth.Candidates(pollCtx, result.Tokens.AccessToken, s.githubSharedAppClientID, s.githubSharedAppSlug)
		if err != nil {
			s.deviceFlows.Fail(lease.FlowID, "GitHub installations could not be verified. Start again.")
			writeGitHubDeviceError(w, "GitHub installations could not be verified", err)
			return
		}
	}
	if pollCtx.Err() != nil {
		s.deviceFlows.Fail(lease.FlowID, "GitHub authorization expired. Start again.")
		writeDeviceStageError(w, "GitHub connection flow expired")
		return
	}
	if err := s.deviceFlows.CompletePoll(lease.FlowID, result, candidates); err != nil {
		s.writeInternalError(w, "complete GitHub authorization poll failed")
		return
	}
	status, _ := s.deviceFlows.Status(lease.FlowID, sessionID, origin, s.githubSharedAppSlug)
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleRefreshGitHubDeviceCandidates(w http.ResponseWriter, r *http.Request) {
	sessionID, origin, ok := s.manifestBrowserBinding(w, r, false)
	if !ok {
		return
	}
	lease, err := s.deviceFlows.BeginCandidateRefresh(r.PathValue("id"), sessionID, origin)
	if err != nil {
		writeDeviceStageError(w, "GitHub installations cannot be refreshed in the current state")
		return
	}
	setSensitiveIdempotencyExpiry(w, lease.ExpiresAt)
	refreshCtx, cancel := contextWithFlowDeadline(r.Context(), s.now, lease.ExpiresAt)
	defer cancel()
	candidates, err := s.githubDeviceOAuth.Candidates(refreshCtx, lease.AccessToken, s.githubSharedAppClientID, s.githubSharedAppSlug)
	if err != nil {
		s.deviceFlows.Fail(r.PathValue("id"), "GitHub installations could not be refreshed. Reconnect to continue.")
		writeGitHubDeviceError(w, "GitHub installations could not be refreshed", err)
		return
	}
	if refreshCtx.Err() != nil {
		s.deviceFlows.Fail(r.PathValue("id"), "GitHub installation refresh expired. Reconnect to continue.")
		writeDeviceStageError(w, "GitHub connection flow expired")
		return
	}
	if err := s.deviceFlows.CompleteCandidateRefresh(r.PathValue("id"), candidates); err != nil {
		s.writeInternalError(w, "complete GitHub installation refresh failed")
		return
	}
	status, _ := s.deviceFlows.Status(r.PathValue("id"), sessionID, origin, s.githubSharedAppSlug)
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleFinalizeGitHubDeviceFlow(w http.ResponseWriter, r *http.Request) {
	sessionID, origin, ok := s.manifestBrowserBinding(w, r, false)
	if !ok {
		return
	}
	var input finalizeDeviceFlowRequest
	if err := decodeStrictJSON(r, &input); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	input.InstallationID, input.DisplayName = strings.TrimSpace(input.InstallationID), strings.TrimSpace(input.DisplayName)
	verification, err := s.deviceFlows.AccessTokenForFinalize(r.PathValue("id"), sessionID, origin)
	if err != nil {
		writeDeviceStageError(w, "Choose an available GitHub installation before continuing")
		return
	}
	setSensitiveIdempotencyExpiry(w, verification.ExpiresAt)
	finalizeCtx, cancel := contextWithFlowDeadline(r.Context(), s.now, verification.ExpiresAt)
	defer cancel()
	fresh, err := s.githubDeviceOAuth.Candidates(finalizeCtx, verification.AccessToken, s.githubSharedAppClientID, s.githubSharedAppSlug)
	if err != nil {
		s.deviceFlows.EndFinalizeVerification(r.PathValue("id"))
		writeGitHubDeviceError(w, "The selected GitHub installation could not be re-verified", err)
		return
	}
	if finalizeCtx.Err() != nil {
		s.deviceFlows.Fail(r.PathValue("id"), "GitHub connection flow expired. Start again.")
		writeDeviceStageError(w, "GitHub connection flow expired")
		return
	}
	lease, candidate, err := s.deviceFlows.BeginFinalize(r.PathValue("id"), sessionID, origin, input.InstallationID, input.DisplayName, fresh)
	if err != nil {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "the selected GitHub installation is no longer available", "api.github_device.finalize", nil)
		return
	}
	connectionID := s.idGenerator("connection")
	at := s.now().UTC().Format(time.RFC3339Nano)
	var connection state.ExternalConnection
	if lease.ReconnectConnectionID != "" {
		connectionID = lease.ReconnectConnectionID
		existing, getErr := s.externalStore.GetConnection(r.Context(), connectionID)
		if getErr != nil || existing.CredentialStrategy != "user_access_token" || existing.AppID != candidate.AppID || existing.ClientID != candidate.ClientID || existing.InstallationID != candidate.InstallationID || existing.InstallationAccountID != candidate.AccountID {
			s.deviceFlows.Fail(lease.FlowID, "The reconnect identity no longer matches. Start again.")
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "the reconnect identity does not match the saved connection", "api.github_device.finalize", nil)
			return
		}
		err = s.githubUserTokens.InstallAudited(connectionID, lease.Tokens, func() error {
			if err := finalizeCtx.Err(); err != nil {
				return err
			}
			var updateErr error
			connection, updateErr = s.externalStore.ReauthorizeSharedConnection(finalizeCtx, existing, candidate.AppSlug, candidate.AccountLogin, authenticatedActorID(r.Context()), s.idGenerator("external_audit"), at)
			return updateErr
		})
	} else {
		err = s.githubUserTokens.InstallAudited(connectionID, lease.Tokens, func() error {
			if err := finalizeCtx.Err(); err != nil {
				return err
			}
			var finalizeErr error
			connection, finalizeErr = s.externalStore.FinalizeConnection(finalizeCtx, state.FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{ConnectionID: connectionID, CredentialStrategy: "user_access_token", DisplayName: candidate.DisplayName, AppID: candidate.AppID, ClientID: candidate.ClientID, AppSlug: candidate.AppSlug, InstallationID: candidate.InstallationID, CredentialHandle: githubexternal.UserCredentialHandle(connectionID), ActorID: authenticatedActorID(r.Context()), CreatedAt: at, AuditEventID: s.idGenerator("external_audit")}, InstallationAccountID: candidate.AccountID, InstallationAccountLogin: candidate.AccountLogin})
			return finalizeErr
		})
	}
	if err != nil {
		s.deviceFlows.Fail(lease.FlowID, "Kingdom could not save the verified connection. Start again.")
		if isUniqueConstraintError(err) || errors.Is(err, state.ErrExternalConflict) {
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, "this GitHub installation is already connected", "api.github_device.finalize", nil)
			return
		}
		s.writeInternalError(w, "finalize shared GitHub connection failed")
		return
	}
	if adapter, ok := s.externalRegistry.Adapter("github"); ok {
		adapter.Evict(connectionID)
	}
	if err := s.deviceFlows.Finalize(lease.FlowID, connectionID); err != nil {
		s.writeInternalError(w, "consume GitHub connection flow failed")
		return
	}
	writeJSON(w, http.StatusCreated, presentConnection(connection, s.githubKeys, s.githubUserTokens))
}

func writeDeviceStageError(w http.ResponseWriter, message string) {
	writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, message, "api.github_device.stage", nil)
}

func writeGitHubDeviceError(w http.ResponseWriter, message string, err error) {
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
			message = "GitHub authorization is invalid or revoked; reconnect and try again"
		case external.FailureRateLimited:
			status = http.StatusTooManyRequests
		case external.FailureTimeout:
			status = http.StatusGatewayTimeout
		case external.FailureUnavailable:
			status = http.StatusServiceUnavailable
		}
	}
	writeCanonicalErrorWithRetryAt(w, status, errorcatalog.CodeDependencyBlocked, message, "api.github_device.github", details, retryAt)
}
