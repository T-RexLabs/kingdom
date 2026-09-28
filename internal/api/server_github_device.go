package api

import (
	"context"
	"errors"
	"net/http"
	"slices"
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
	InstallationIDs []string `json:"installation_ids"`
	DisplayName     string   `json:"display_name"`
}

const githubDeviceOperationTimeout = 30 * time.Second

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
	principal, _ := authenticatedPrincipal(r.Context())
	flow, startLease, reserved, err := s.deviceFlows.ReserveStart(sessionID, origin, input.ConnectionID, principal.ExpiresAt)
	if err != nil {
		switch {
		case errors.Is(err, errDeviceFlowCapacity):
			writeCanonicalError(w, http.StatusTooManyRequests, errorcatalog.CodeDependencyBlocked, "too many GitHub connection flows are active", "api.github_device.start", nil)
		case errors.Is(err, errDeviceFlowIntentConflict):
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "another GitHub setup or reconnect is already active in this browser session; resume or discard it first", "api.github_device.start", nil)
		case errors.Is(err, errDeviceFlowInFlight):
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "GitHub authorization is already being started for this browser session", "api.github_device.start", nil)
		default:
			s.writeInternalError(w, "reserve GitHub device flow failed")
		}
		return
	}
	if !reserved {
		setSensitiveIdempotencyExpiry(w, flow.ExpiresAt)
		writeJSON(w, http.StatusOK, statusFromDeviceFlow(flow, s.githubSharedAppSlug))
		return
	}
	startCtx, cancel := contextWithFlowDeadline(s.lifecycleCtx, s.now, flow.ExpiresAt, githubDeviceOperationTimeout)
	defer cancel()
	authorization, err := s.githubDeviceOAuth.Start(startCtx, s.githubSharedAppClientID)
	if err != nil {
		s.deviceFlows.AbortStart(startLease)
		writeGitHubDeviceError(w, "GitHub authorization could not be started", err)
		return
	}
	flow, err = s.deviceFlows.CompleteStart(startLease, authorization)
	if err != nil {
		s.deviceFlows.AbortStart(startLease)
		writeDeviceStageError(w, "GitHub authorization expired before it could start")
		return
	}
	setSensitiveIdempotencyExpiry(w, flow.ExpiresAt)
	writeJSON(w, http.StatusCreated, statusFromDeviceFlow(flow, s.githubSharedAppSlug))
}

func (s *Server) handleCurrentGitHubDeviceFlow(w http.ResponseWriter, r *http.Request) {
	sessionID, origin, ok := s.manifestBrowserBinding(w, r, false)
	if !ok {
		return
	}
	status, err := s.deviceFlows.Current(sessionID, origin, s.githubSharedAppSlug)
	if err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, status)
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

func (s *Server) handleDiscardGitHubDeviceFlow(w http.ResponseWriter, r *http.Request) {
	sessionID, origin, ok := s.manifestBrowserBinding(w, r, true)
	if !ok {
		return
	}
	err := s.deviceFlows.Discard(r.PathValue("id"), sessionID, origin)
	if errors.Is(err, errDeviceFlowInFlight) {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "GitHub connection flow work is in progress and cannot be discarded yet", "api.github_device.discard", nil)
		return
	}
	if err != nil {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "GitHub connection flow is unavailable", "api.github_device.discard", nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
	pollCtx, cancel := contextWithFlowDeadline(s.lifecycleCtx, s.now, lease.ExpiresAt, githubDeviceOperationTimeout)
	defer cancel()
	result, err := s.githubDeviceOAuth.Poll(pollCtx, s.githubSharedAppClientID, lease.DeviceCode)
	if err != nil {
		_ = s.deviceFlows.Fail(lease.deviceOperationLease, "GitHub authorization could not be completed. Start again.")
		writeGitHubDeviceError(w, "GitHub authorization failed", err)
		return
	}
	var candidates []githubexternal.InstallationCandidate
	if result.Status == githubexternal.DevicePollAuthorized {
		candidates, err = s.githubDeviceOAuth.Candidates(pollCtx, result.Tokens.AccessToken, s.githubSharedAppClientID, s.githubSharedAppSlug)
		if err != nil {
			_ = s.deviceFlows.Fail(lease.deviceOperationLease, "GitHub installations could not be verified. Start again.")
			writeGitHubDeviceError(w, "GitHub installations could not be verified", err)
			return
		}
	}
	if pollCtx.Err() != nil {
		_ = s.deviceFlows.Fail(lease.deviceOperationLease, "GitHub authorization expired. Start again.")
		writeDeviceStageError(w, "GitHub connection flow expired")
		return
	}
	status, err := s.deviceFlows.CompletePoll(lease, result, candidates, s.githubSharedAppSlug)
	if err != nil {
		s.writeInternalError(w, "complete GitHub authorization poll failed")
		return
	}
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
	refreshCtx, cancel := contextWithFlowDeadline(s.lifecycleCtx, s.now, lease.ExpiresAt, githubDeviceOperationTimeout)
	defer cancel()
	candidates, err := s.githubDeviceOAuth.Candidates(refreshCtx, lease.AccessToken, s.githubSharedAppClientID, s.githubSharedAppSlug)
	if err != nil {
		_ = s.deviceFlows.Fail(lease.deviceOperationLease, "GitHub installations could not be refreshed. Reconnect to continue.")
		writeGitHubDeviceError(w, "GitHub installations could not be refreshed", err)
		return
	}
	if refreshCtx.Err() != nil {
		_ = s.deviceFlows.Fail(lease.deviceOperationLease, "GitHub installation refresh expired. Reconnect to continue.")
		writeDeviceStageError(w, "GitHub connection flow expired")
		return
	}
	status, err := s.deviceFlows.CompleteCandidateRefresh(lease, candidates, s.githubSharedAppSlug)
	if err != nil {
		s.writeInternalError(w, "complete GitHub installation refresh failed")
		return
	}
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
	input.DisplayName = strings.TrimSpace(input.DisplayName)
	for i := range input.InstallationIDs {
		input.InstallationIDs[i] = strings.TrimSpace(input.InstallationIDs[i])
	}
	if input.DisplayName == "" || len(input.InstallationIDs) == 0 {
		s.writeBadRequest(w, "choose at least one installation and name the connection")
		return
	}
	reconnectID, err := s.deviceFlows.ReconnectIntent(r.PathValue("id"), sessionID, origin)
	if err != nil {
		writeDeviceStageError(w, "Choose an available GitHub installation before continuing")
		return
	}
	var reconnectConnection state.ExternalConnection
	if reconnectID != "" {
		reconnectConnection, err = s.externalStore.GetConnection(r.Context(), reconnectID)
		if err != nil || reconnectConnection.CredentialStrategy != "user_access_token" || reconnectConnection.Lifecycle == "removed" || reconnectConnection.Lifecycle == "removal_pending" {
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "the saved shared GitHub connection is no longer available for reconnect", "api.github_device.finalize", nil)
			return
		}
		if missing, extra := installationSetDifference(reconnectConnection.InstallationIDs(), input.InstallationIDs); len(missing) != 0 || len(extra) != 0 {
			writeReconnectSetConflict(w, missing, extra)
			return
		}
		input.InstallationIDs = reconnectConnection.InstallationIDs()
	}
	verification, err := s.deviceFlows.BeginFinalizeVerification(r.PathValue("id"), sessionID, origin)
	if err != nil {
		writeDeviceStageError(w, "Choose an available GitHub installation before continuing")
		return
	}
	setSensitiveIdempotencyExpiry(w, verification.ExpiresAt)
	finalizeCtx, cancel := contextWithFlowDeadline(s.lifecycleCtx, s.now, verification.ExpiresAt, githubDeviceOperationTimeout)
	defer cancel()
	actorID := authenticatedActorID(r.Context())
	fresh, err := s.githubDeviceOAuth.Candidates(finalizeCtx, verification.AccessToken, s.githubSharedAppClientID, s.githubSharedAppSlug)
	if err != nil {
		s.deviceFlows.EndFinalizeVerification(verification)
		writeGitHubDeviceError(w, "The selected GitHub installation could not be re-verified", err)
		return
	}
	if finalizeCtx.Err() != nil {
		_ = s.deviceFlows.Fail(verification.deviceOperationLease, "GitHub connection flow expired. Start again.")
		writeDeviceStageError(w, "GitHub connection flow expired")
		return
	}
	if reconnectID != "" {
		selected, missing, identityMismatch := reconnectCandidates(reconnectConnection, fresh)
		if len(missing) != 0 || identityMismatch {
			s.deviceFlows.EndFinalizeVerification(verification)
			details := map[string]any{}
			message := "GitHub authorization no longer matches the saved connection identity"
			if len(missing) != 0 {
				details["missing_installation_ids"] = missing
				message = "GitHub authorization cannot access every saved installation; restore access or use Change before reconnecting"
			}
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, message, "api.github_device.finalize", details)
			return
		}
		fresh = selected
	}
	lease, selected, err := s.deviceFlows.CompleteFinalizeVerification(verification, input.InstallationIDs, fresh)
	if err != nil {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "the selected GitHub installation is no longer available", "api.github_device.finalize", nil)
		return
	}
	connectionID := s.idGenerator("connection")
	at := s.now().UTC().Format(time.RFC3339Nano)
	var connection state.ExternalConnection
	if lease.ReconnectConnectionID != "" {
		connectionID = lease.ReconnectConnectionID
		existing := reconnectConnection
		if existing.ConnectionID != connectionID || existing.AppID != selected[0].AppID || existing.ClientID != selected[0].ClientID {
			_ = s.deviceFlows.Fail(lease.deviceOperationLease, "The reconnect identity no longer matches. Start again.")
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "the reconnect identity does not match the saved connection", "api.github_device.finalize", nil)
			return
		}
		targetGeneration := existing.CredentialGeneration + 1
		var reconciliationErr error
		err = s.githubUserTokens.InstallAudited(connectionID, targetGeneration, lease.Tokens, func() error {
			if err := finalizeCtx.Err(); err != nil {
				return err
			}
			var updateErr error
			connection, updateErr = s.externalStore.ReauthorizeSharedConnection(finalizeCtx, existing, selected[0].AppSlug, installationsFromCandidates(selected), actorID, s.idGenerator("external_audit"), at)
			if updateErr != nil {
				published, publicationErr := s.sharedReauthorizationPublished(finalizeCtx, connectionID, targetGeneration)
				if publicationErr == nil && published {
					return nil
				}
				if publicationErr != nil {
					reconciliationErr = publicationErr
				}
			}
			return updateErr
		})
		if err != nil && reconciliationErr != nil {
			_ = s.deviceFlows.Fail(lease.deviceOperationLease, "The reconnect outcome could not be confirmed. Check the saved connection before starting another reconnect flow.")
			writeCommitOutcomePending(w, r, "api.github_device.finalize",
				"GitHub reconnect outcome is still being confirmed; retry later with the same idempotency key",
				map[string]any{"connection_id": connectionID})
			return
		}
	} else {
		err = s.githubUserTokens.InstallAudited(connectionID, 1, lease.Tokens, func() error {
			if err := finalizeCtx.Err(); err != nil {
				return err
			}
			var finalizeErr error
			connection, finalizeErr = s.externalStore.FinalizeConnection(finalizeCtx, state.FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{ConnectionID: connectionID, CredentialStrategy: "user_access_token", DisplayName: input.DisplayName, AppID: selected[0].AppID, ClientID: selected[0].ClientID, AppSlug: selected[0].AppSlug, Installations: installationsFromCandidates(selected), CredentialHandle: githubexternal.UserCredentialHandle(connectionID), ActorID: actorID, CreatedAt: at, AuditEventID: s.idGenerator("external_audit")}})
			if finalizeErr == nil {
				return nil
			}
			published, publicationErr := s.connectionCredentialPublished(finalizeCtx, connectionID, "user_access_token", 1)
			if publicationErr == nil && published {
				return nil
			}
			if connection.ConnectionID == connectionID && !errors.Is(publicationErr, state.ErrExternalNotFound) {
				return &state.CommitOutcomeUnknownError{
					Operation: "finalize shared GitHub connection", ResourceID: connectionID,
					Cause: errors.Join(finalizeErr, publicationErr),
				}
			}
			return finalizeErr
		})
	}
	if err != nil {
		_ = s.deviceFlows.Fail(lease.deviceOperationLease, "The connection outcome could not be confirmed. Check saved connections before starting a new connection flow.")
		var commitErr *state.CommitOutcomeUnknownError
		if errors.As(err, &commitErr) {
			connectionID := strings.TrimSpace(commitErr.ResourceID)
			writeCommitOutcomePending(w, r, "api.github_device.finalize",
				"GitHub connection outcome is still being confirmed; retry later with the same idempotency key",
				map[string]any{"connection_id": connectionID})
			return
		}
		if isUniqueConstraintError(err) || errors.Is(err, state.ErrExternalConflict) {
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, "one of these GitHub installations is already connected", "api.github_device.finalize", nil)
			return
		}
		s.writeInternalError(w, "finalize shared GitHub connection failed")
		return
	}
	if adapter, ok := s.externalRegistry.Adapter("github"); ok {
		adapter.Evict(connectionID)
	}
	if err := s.deviceFlows.Finalize(lease, connectionID); err != nil {
		s.writeInternalError(w, "consume GitHub connection flow failed")
		return
	}
	writeJSON(w, http.StatusCreated, presentConnection(connection, s.githubKeys, s.githubUserTokens))
}

func (s *Server) sharedReauthorizationPublished(ctx context.Context, connectionID string, targetGeneration int) (bool, error) {
	return s.connectionCredentialPublished(ctx, connectionID, "user_access_token", targetGeneration)
}

func (s *Server) connectionCredentialPublished(ctx context.Context, connectionID, strategy string, targetGeneration int) (bool, error) {
	readbackCtx, cancel := commitOutcomeReadbackContext(ctx)
	defer cancel()
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		publication, err := s.externalStore.GetCredentialPublication(readbackCtx, connectionID)
		if errors.Is(err, state.ErrExternalNotFound) {
			return false, err
		}
		if err != nil {
			lastErr = err
			if readbackCtx.Err() != nil {
				break
			}
			continue
		}
		return publication.CredentialStrategy == strategy && publication.CredentialGeneration == targetGeneration, nil
	}
	return false, lastErr
}

func installationSetDifference(expected, actual []string) (missing, extra []string) {
	expectedSet := make(map[string]struct{}, len(expected))
	actualSet := make(map[string]struct{}, len(actual))
	for _, id := range expected {
		expectedSet[strings.TrimSpace(id)] = struct{}{}
	}
	for _, id := range actual {
		actualSet[strings.TrimSpace(id)] = struct{}{}
	}
	for id := range expectedSet {
		if _, ok := actualSet[id]; !ok {
			missing = append(missing, id)
		}
	}
	for id := range actualSet {
		if _, ok := expectedSet[id]; !ok {
			extra = append(extra, id)
		}
	}
	slices.Sort(missing)
	slices.Sort(extra)
	return missing, extra
}

func writeReconnectSetConflict(w http.ResponseWriter, missing, extra []string) {
	details := map[string]any{}
	parts := make([]string, 0, 2)
	if len(missing) != 0 {
		details["missing_installation_ids"] = missing
		parts = append(parts, "include every saved installation")
	}
	if len(extra) != 0 {
		details["extra_installation_ids"] = extra
		parts = append(parts, "remove additional installations and use Change to edit the saved set")
	}
	writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "reconnect must "+strings.Join(parts, " and "), "api.github_device.finalize", details)
}

func reconnectCandidates(connection state.ExternalConnection, fresh []githubexternal.InstallationCandidate) (selected []githubexternal.InstallationCandidate, missing []string, identityMismatch bool) {
	byID := make(map[string]githubexternal.InstallationCandidate, len(fresh))
	for _, candidate := range fresh {
		byID[candidate.InstallationID] = candidate
	}
	for _, saved := range connection.Installations {
		candidate, ok := byID[saved.InstallationID]
		if !ok {
			missing = append(missing, saved.InstallationID)
			continue
		}
		if candidate.AccountID != saved.AccountID || candidate.AppID != connection.AppID || candidate.ClientID != connection.ClientID {
			identityMismatch = true
			continue
		}
		selected = append(selected, candidate)
	}
	slices.Sort(missing)
	return selected, missing, identityMismatch
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

// installationsFromCandidates turns freshly proved candidates into the covered
// set a connection stores.
func installationsFromCandidates(candidates []githubexternal.InstallationCandidate) []state.ConnectionInstallation {
	out := make([]state.ConnectionInstallation, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, state.ConnectionInstallation{InstallationID: candidate.InstallationID, AccountID: candidate.AccountID, AccountLogin: candidate.AccountLogin})
	}
	return out
}
