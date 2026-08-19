package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/external"
	githubexternal "kingdom/internal/external/github"
	"kingdom/internal/state"
)

func presentConnection(c state.ExternalConnection, keys *githubexternal.KeyStore, userTokens *githubexternal.UserTokenStore) gen.Connection {
	credentialState := "missing"
	if c.Lifecycle != "removed" && c.Lifecycle != "removal_pending" {
		if c.CredentialStrategy == "app_private_key" && keys != nil {
			if _, _, err := keys.Load(c.ConnectionID); err == nil {
				credentialState = "present"
			}
		} else if c.CredentialStrategy == "user_access_token" && userTokens != nil {
			if _, err := userTokens.Load(c.ConnectionID); err == nil {
				credentialState = "present"
			}
		}
	}
	if credentialState == "missing" && c.Lifecycle == "enabled" {
		c.HealthClass = "missing_credential"
		c.FailureClass = ""
	}
	result := gen.Connection{
		ConnectionId: c.ConnectionID, Provider: gen.ConnectionProvider(c.Provider), CredentialStrategy: gen.ConnectionCredentialStrategy(c.CredentialStrategy), DisplayName: c.DisplayName,
		AppId: c.AppID, ClientId: c.ClientID, AppSlug: c.AppSlug, InstallationId: c.InstallationID,
		Lifecycle: gen.ConnectionLifecycle(c.Lifecycle), HealthClass: gen.ConnectionHealthClass(c.HealthClass),
		CredentialState: gen.ConnectionCredentialState(credentialState), CreatedAt: mustParseRFC3339(c.CreatedAt), UpdatedAt: mustParseRFC3339(c.UpdatedAt),
	}
	if c.InstallationAccountID != "" {
		result.InstallationAccountId = &c.InstallationAccountID
		result.InstallationAccountLogin = &c.InstallationAccountLogin
	}
	if c.FailureClass != "" {
		result.FailureClass = &c.FailureClass
	}
	if c.LastCheckedAt != nil {
		value := mustParseRFC3339(*c.LastCheckedAt)
		result.LastCheckedAt = &value
	}
	if c.LastSuccessAt != nil {
		value := mustParseRFC3339(*c.LastSuccessAt)
		result.LastSuccessAt = &value
	}
	return result
}

func (s *Server) handleListConnections(w http.ResponseWriter, r *http.Request) {
	items, err := s.externalStore.ListConnections(r.Context())
	if err != nil {
		s.writeInternalError(w, "list connections failed")
		return
	}
	out := make([]gen.Connection, 0, len(items))
	for _, v := range items {
		out = append(out, presentConnection(v, s.githubKeys, s.githubUserTokens))
	}
	writeJSON(w, http.StatusOK, gen.ConnectionList{Items: out})
}
func (s *Server) handleGetConnection(w http.ResponseWriter, r *http.Request) {
	v, err := s.externalStore.GetConnection(r.Context(), r.PathValue("id"))
	if err != nil {
		writeExternalError(w, err, "api.get_connection")
		return
	}
	writeJSON(w, http.StatusOK, presentConnection(v, s.githubKeys, s.githubUserTokens))
}

func (s *Server) handleCreateConnection(w http.ResponseWriter, r *http.Request) {
	var req gen.CreateConnectionRequest
	if err := decodeStrictJSON(r, &req); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	if req.Provider != gen.CreateConnectionRequestProvider("github") {
		s.writeBadRequest(w, "provider must be github")
		return
	}
	if req.CredentialStrategy != gen.CreateConnectionRequestCredentialStrategy("app_private_key") {
		s.writeBadRequest(w, "credential_strategy must be app_private_key")
		return
	}
	now := s.now().UTC().Format(time.RFC3339Nano)
	id := s.idGenerator("connection")
	appSlug := ""
	if req.AppSlug != nil {
		appSlug = *req.AppSlug
	}
	row, err := s.externalStore.CreateConnection(r.Context(), state.CreateExternalConnectionRequest{ConnectionID: id, CredentialStrategy: "app_private_key", DisplayName: req.DisplayName, AppID: req.AppId, ClientID: req.ClientId, AppSlug: appSlug, InstallationID: req.InstallationId, CredentialHandle: githubexternal.CredentialHandle(id), ActorID: authenticatedActorID(r.Context()), CreatedAt: now, AuditEventID: s.idGenerator("external_audit")})
	if err != nil {
		if isUniqueConstraintError(err) {
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, "connection already exists", "api.create_connection", nil)
			return
		}
		s.writeBadRequest(w, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, presentConnection(row, s.githubKeys, s.githubUserTokens))
}

func (s *Server) handleUpdateConnection(w http.ResponseWriter, r *http.Request) {
	var req gen.UpdateConnectionRequest
	if err := decodeStrictJSON(r, &req); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	now := s.now().UTC().Format(time.RFC3339Nano)
	row, err := s.externalStore.UpdateConnection(r.Context(), r.PathValue("id"), state.UpdateExternalConnectionRequest{Enabled: req.Enabled, DisplayName: req.DisplayName, ActorID: authenticatedActorID(r.Context()), UpdatedAt: now, AuditEventID: s.idGenerator("external_audit")})
	if err != nil {
		if isUniqueConstraintError(err) {
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, "connection installation already exists", "api.update_connection", nil)
			return
		}
		writeExternalError(w, err, "api.update_connection")
		return
	}
	if row.Lifecycle != "enabled" {
		if adapter, ok := s.externalRegistry.Adapter("github"); ok {
			adapter.Evict(r.PathValue("id"))
		}
	}
	writeJSON(w, http.StatusOK, presentConnection(row, s.githubKeys, s.githubUserTokens))
}

// maxIncludedRepositories bounds one narrowing request. GitHub's own
// pre-selection ceiling is 100 repositories; this leaves generous headroom
// while keeping a single request finite.
const maxIncludedRepositories = 500

func (s *Server) handleSetIncludedRepositories(w http.ResponseWriter, r *http.Request) {
	conn, adapter, ok := s.connectionAdapter(w, r)
	if !ok {
		return
	}
	var req gen.SetIncludedRepositoriesRequest
	if err := decodeStrictJSON(r, &req); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	mode := string(req.Mode)
	requested := map[string]struct{}{}
	if req.RepositoryIds != nil {
		for _, id := range *req.RepositoryIds {
			requested[strings.TrimSpace(id)] = struct{}{}
		}
	}
	if mode == "all" {
		requested = map[string]struct{}{}
	}
	if mode == "selected" && len(requested) == 0 {
		s.writeBadRequest(w, "select at least one repository, or include all of them")
		return
	}
	if len(requested) > maxIncludedRepositories {
		s.writeBadRequest(w, "too many repositories in one request")
		return
	}

	// A connection may only narrow its installation grant, never widen it, so
	// every requested repository is proven against the live grant first.
	resolved := make([]state.IncludedRepository, 0, len(requested))
	if mode == "selected" {
		found := make(map[string]struct{}, len(requested))
		cursor := ""
		for page := 0; page < 100 && len(found) < len(requested); page++ {
			granted, err := adapter.ListRepositories(r.Context(), conn, "", cursor)
			if err != nil {
				writeExternalError(w, err, "api.set_included_repositories")
				return
			}
			for _, repository := range granted.Items {
				if _, wanted := requested[repository.ID]; !wanted {
					continue
				}
				if _, already := found[repository.ID]; already {
					continue
				}
				found[repository.ID] = struct{}{}
				resolved = append(resolved, state.IncludedRepository{RepositoryNativeID: repository.ID, RepositoryLocator: repository.Locator})
			}
			if granted.NextCursor == "" {
				break
			}
			cursor = granted.NextCursor
		}
		if len(found) != len(requested) {
			writeExternalError(w, state.ErrExternalRepositoryExcluded, "api.set_included_repositories")
			return
		}
	}

	row, err := s.externalStore.SetIncludedRepositories(r.Context(), conn.ConnectionID, state.SetIncludedRepositoriesRequest{
		Mode: mode, Repositories: resolved, ExpectedRowVersion: conn.RowVersion,
		ActorID: authenticatedActorID(r.Context()), UpdatedAt: s.now().UTC().Format(time.RFC3339Nano), AuditEventID: s.idGenerator("external_audit"),
	})
	if err != nil {
		writeExternalError(w, err, "api.set_included_repositories")
		return
	}
	writeJSON(w, http.StatusOK, presentConnection(row, s.githubKeys, s.githubUserTokens))
}

func (s *Server) handleDeleteConnection(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	now := s.now().UTC().Format(time.RFC3339Nano)
	remove := func() error {
		if err := s.externalStore.UpdateConnectionStateAudited(r.Context(), id, "removal_pending", "removal_pending", "", now, false, authenticatedActorID(r.Context()), s.idGenerator("external_audit"), "connection.removal_pending"); err != nil {
			return err
		}
		if adapter, ok := s.externalRegistry.Adapter("github"); ok {
			adapter.Evict(id)
		}
		if connection, getErr := s.externalStore.GetConnection(r.Context(), id); getErr == nil && connection.CredentialStrategy == "user_access_token" {
			if s.githubUserTokens != nil {
				if err := s.githubUserTokens.RemoveWhileLocked(id); err != nil {
					return err
				}
			}
		} else if s.githubKeys != nil {
			if err := s.githubKeys.Remove(id); err != nil {
				return err
			}
		}
		return s.externalStore.UpdateConnectionStateAudited(r.Context(), id, "removed", "removed", "", now, false, authenticatedActorID(r.Context()), s.idGenerator("external_audit"), "connection.removed")
	}
	var err error
	connection, _ := s.externalStore.GetConnection(r.Context(), id)
	if connection.CredentialStrategy == "user_access_token" && s.githubUserTokens != nil {
		err = s.githubUserTokens.WithConnectionLock(id, remove)
	} else if s.githubKeys != nil {
		err = s.githubKeys.WithConnectionLock(id, remove)
	} else {
		err = remove()
	}
	if err != nil {
		if errors.Is(err, state.ErrExternalNotFound) || errors.Is(err, state.ErrExternalConflict) {
			writeExternalError(w, err, "api.delete_connection")
			return
		}
		s.writeInternalError(w, "finalize connection removal failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleConnectionHealthCheck(w http.ResponseWriter, r *http.Request) {
	conn, adapter, ok := s.enabledConnectionAdapter(w, r)
	if !ok {
		return
	}
	now := s.now().UTC().Format(time.RFC3339Nano)
	validation, err := adapter.Validate(r.Context(), conn)
	if validation.AppSlug == "" {
		validation.AppSlug = conn.AppSlug
	}
	health, failure, success := "healthy", "", true
	if err != nil {
		health, failure, success = providerHealth(err), providerFailure(err), false
	}
	if stateErr := s.externalStore.UpdateConnectionStateAuditedVersionIdentity(r.Context(), conn.ConnectionID, conn.Lifecycle, health, failure, now, success, validation.AppSlug, validation.InstallationAccountID, validation.InstallationAccountLogin, authenticatedActorID(r.Context()), s.idGenerator("external_audit"), "connection.health_checked", conn.RowVersion); stateErr != nil {
		writeExternalError(w, stateErr, "api.connection_health_check")
		return
	}
	if err != nil {
		writeConnectionHealthError(w, err)
		return
	}
	updated, _ := s.externalStore.GetConnection(r.Context(), conn.ConnectionID)
	writeJSON(w, http.StatusOK, presentConnection(updated, s.githubKeys, s.githubUserTokens))
}

func writeConnectionHealthError(w http.ResponseWriter, err error) {
	var providerErr *external.ProviderError
	if errors.As(err, &providerErr) {
		status := http.StatusBadGateway
		message := "GitHub App validation failed; verify the App installation and try again"
		details := map[string]any{"provider_failure_class": string(providerErr.Class)}
		switch providerErr.Class {
		case external.FailureAuthentication, external.FailureInaccessibleOrMissing:
			status = http.StatusFailedDependency
			message = "GitHub authorization is invalid or revoked; reconnect the connection, then run the connection health check"
		case external.FailurePermissionMismatch:
			status = http.StatusForbidden
			message = "GitHub App must have exactly repository metadata read and Issues read, zero subscribed events, and no additional permissions"
		case external.FailureRateLimited:
			status = http.StatusTooManyRequests
			message = "GitHub rate limit blocked the connection check; retry after the reported reset"
		case external.FailureTimeout, external.FailureUnavailable:
			status = http.StatusServiceUnavailable
			message = "GitHub App validation is temporarily unavailable; retry the connection check"
		}
		writeCanonicalErrorWithRetryAt(w, status, errorcatalog.CodeDependencyBlocked, message, "api.connection_health_check", details, providerErr.RetryAt)
		return
	}
	writeExternalError(w, err, "api.connection_health_check")
}
func (s *Server) connectionAdapter(w http.ResponseWriter, r *http.Request) (state.ExternalConnection, external.Adapter, bool) {
	conn, adapter, ok := s.enabledConnectionAdapter(w, r)
	if !ok {
		return conn, adapter, false
	}
	if !conn.HasCurrentValidation() {
		writeExternalError(w, state.ErrExternalValidationNeeded, "api.connection")
		return conn, nil, false
	}
	return conn, adapter, true
}

func (s *Server) enabledConnectionAdapter(w http.ResponseWriter, r *http.Request) (state.ExternalConnection, external.Adapter, bool) {
	conn, err := s.externalStore.GetConnection(r.Context(), r.PathValue("id"))
	if err != nil {
		writeExternalError(w, err, "api.connection")
		return conn, nil, false
	}
	if conn.Lifecycle != "enabled" {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "connection is disabled", "api.connection", nil)
		return conn, nil, false
	}
	adapter, ok := s.externalRegistry.Adapter(conn.Provider)
	if !ok {
		writeCanonicalError(w, http.StatusServiceUnavailable, errorcatalog.CodeDependencyBlocked, "provider is unavailable", "api.connection", nil)
		return conn, nil, false
	}
	return conn, adapter, true
}

// includedRepositoryIDs reports the narrowing a connection applies to its
// installation grant. A connection in "all" mode returns narrowed=false and no
// set, because it deliberately stores nothing and tracks whatever GitHub grants.
//
// The narrowing is applied here in the API layer and never in the provider
// adapter: connection validation and transfer resolution both call
// ListRepositories and must keep seeing the true grant.
func (s *Server) includedRepositoryIDs(ctx context.Context, conn state.ExternalConnection) (map[string]struct{}, bool, error) {
	if conn.IncludedRepositoryMode != "selected" {
		return nil, false, nil
	}
	included, err := s.externalStore.GetIncludedRepositories(ctx, conn.ConnectionID)
	if err != nil {
		return nil, false, err
	}
	ids := make(map[string]struct{}, len(included))
	for _, item := range included {
		ids[item.RepositoryNativeID] = struct{}{}
	}
	return ids, true, nil
}

// requireIncludedRepository rejects a repository the connection excludes, so an
// exclusion is real rather than cosmetic.
func (s *Server) requireIncludedRepository(ctx context.Context, conn state.ExternalConnection, repositoryID string) error {
	ids, narrowed, err := s.includedRepositoryIDs(ctx, conn)
	if err != nil || !narrowed {
		return err
	}
	if _, ok := ids[strings.TrimSpace(repositoryID)]; !ok {
		return state.ErrExternalRepositoryExcluded
	}
	return nil
}

func (s *Server) handleListConnectionRepositories(w http.ResponseWriter, r *http.Request) {
	conn, adapter, ok := s.connectionAdapter(w, r)
	if !ok {
		return
	}
	search := strings.TrimSpace(r.URL.Query().Get("search"))
	scope := strings.TrimSpace(r.URL.Query().Get("scope"))
	if scope == "" {
		scope = "included"
	}
	if scope != "included" && scope != "granted" {
		s.writeBadRequest(w, "scope must be included or granted")
		return
	}
	cursorScope := conn.ConnectionID + "\n" + scope + "\n" + search
	providerCursor := ""
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		var decodeErr error
		providerCursor, decodeErr = s.cursors.Decode("connection-repositories", cursorScope, raw)
		if decodeErr != nil {
			s.writeBadRequest(w, "invalid repositories cursor")
			return
		}
	}
	included, narrowed, err := s.includedRepositoryIDs(r.Context(), conn)
	if err != nil {
		writeExternalError(w, err, "api.list_connection_repositories")
		return
	}
	page, err := adapter.ListRepositories(r.Context(), conn, search, providerCursor)
	if err != nil {
		writeExternalError(w, err, "api.list_connection_repositories")
		return
	}
	items := make([]map[string]any, 0, len(page.Items))
	for _, repository := range page.Items {
		_, isIncluded := included[repository.ID]
		isIncluded = isIncluded || !narrowed
		if scope == "included" && !isIncluded {
			continue
		}
		item := map[string]any{"id": repository.ID, "locator": repository.Locator, "canonical_url": repository.CanonicalURL}
		if scope == "granted" {
			item["included"] = isIncluded
		}
		items = append(items, item)
	}
	response := map[string]any{"items": items}
	if page.NextCursor != "" {
		response["next_cursor"] = s.cursors.Encode("connection-repositories", cursorScope, page.NextCursor)
	}
	writeJSON(w, http.StatusOK, response)
}
func (s *Server) handleListConnectionIssues(w http.ResponseWriter, r *http.Request) {
	conn, adapter, ok := s.connectionAdapter(w, r)
	if !ok {
		return
	}
	repositoryID, stateFilter, search := r.PathValue("repositoryId"), r.URL.Query().Get("state"), r.URL.Query().Get("search")
	if err := s.requireIncludedRepository(r.Context(), conn, repositoryID); err != nil {
		writeExternalError(w, err, "api.list_connection_issues")
		return
	}
	cursorScope := conn.ConnectionID + "\n" + repositoryID + "\n" + stateFilter + "\n" + search
	providerCursor := ""
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		var decodeErr error
		providerCursor, decodeErr = s.cursors.Decode("connection-issues", cursorScope, raw)
		if decodeErr != nil {
			s.writeBadRequest(w, "invalid issues cursor")
			return
		}
	}
	page, err := adapter.ListIssues(r.Context(), conn, repositoryID, stateFilter, search, providerCursor)
	if err != nil {
		writeExternalError(w, err, "api.list_connection_issues")
		return
	}
	response := map[string]any{"items": page.Items, "incomplete_results": page.IncompleteResults, "search_result_cap": page.SearchResultCap}
	if page.NextCursor != "" {
		response["next_cursor"] = s.cursors.Encode("connection-issues", cursorScope, page.NextCursor)
	}
	writeJSON(w, http.StatusOK, response)
}

func importProfileFromQuery(r *http.Request) state.ExternalImportProfile {
	comments := false
	if raw := r.URL.Query().Get("include_comments"); raw != "" {
		comments, _ = strconv.ParseBool(raw)
	}
	return state.ExternalImportProfile{IncludeComments: comments}
}
func (s *Server) handlePreviewConnectionIssue(w http.ResponseWriter, r *http.Request) {
	conn, adapter, ok := s.connectionAdapter(w, r)
	if !ok {
		return
	}
	number, err := strconv.Atoi(r.PathValue("number"))
	if err != nil || number < 1 {
		s.writeBadRequest(w, "issue number must be positive")
		return
	}
	if err := s.requireIncludedRepository(r.Context(), conn, r.PathValue("repositoryId")); err != nil {
		writeExternalError(w, err, "api.preview_connection_issue")
		return
	}
	result, err := adapter.FetchIssue(r.Context(), external.IssueRequest{Connection: conn, RepositoryID: r.PathValue("repositoryId"), IssueNumber: number, Profile: importProfileFromQuery(r)})
	if err != nil {
		writeExternalError(w, err, "api.preview_connection_issue")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, presentIssueResult(result))
}

func profileFromCapture(req gen.ExternalIntake) state.ExternalImportProfile {
	comments := false
	if req.IncludeComments != nil {
		comments = *req.IncludeComments
	}
	return state.ExternalImportProfile{IncludeComments: comments}
}
func (s *Server) fetchCapture(r *http.Request, req gen.ExternalIntake) (state.CaptureExternalIssueRequest, error) {
	conn, err := s.externalStore.GetConnection(r.Context(), req.ConnectionId)
	if err != nil {
		return state.CaptureExternalIssueRequest{}, err
	}
	if conn.Lifecycle != "enabled" {
		return state.CaptureExternalIssueRequest{}, state.ErrExternalConnectionUnavailable
	}
	if !conn.HasCurrentValidation() {
		return state.CaptureExternalIssueRequest{}, state.ErrExternalValidationNeeded
	}
	if err := s.requireIncludedRepository(r.Context(), conn, req.RepositoryId); err != nil {
		return state.CaptureExternalIssueRequest{}, err
	}
	adapter, ok := s.externalRegistry.Adapter(conn.Provider)
	if !ok {
		return state.CaptureExternalIssueRequest{}, fmt.Errorf("provider unavailable")
	}
	result, err := adapter.FetchIssue(r.Context(), external.IssueRequest{Connection: conn, RepositoryID: req.RepositoryId, IssueNumber: req.IssueNumber, Profile: profileFromCapture(req)})
	if err != nil {
		return state.CaptureExternalIssueRequest{}, err
	}
	now := s.now().UTC().Format(time.RFC3339Nano)
	return state.CaptureExternalIssueRequest{SourceID: s.idGenerator("external_source"), SnapshotID: s.idGenerator("external_snapshot"), ConnectionID: conn.ConnectionID, ConnectionRowVersion: conn.RowVersion, ProviderNativeID: result.NativeID, RepositoryNativeID: result.RepositoryID, Issue: result.Issue, Profile: result.Profile, ProviderUpdatedAt: result.Issue.UpdatedAt, FetchedAt: now, ETag: result.ETag, CommentsETag: result.CommentsETag, LastModified: result.LastModified, ProviderCommentCount: result.ProviderCommentCount, FetchedCommentCount: result.FetchedCommentCount, FetchedCommentPages: result.FetchedCommentPages, OmittedCommentCount: result.OmittedCommentCount, OmittedCommentPages: result.OmittedCommentPages, Comments: result.Comments}, nil
}
func (s *Server) handleCaptureExternalSource(w http.ResponseWriter, r *http.Request) {
	var req gen.ExternalIntake
	if err := decodeStrictJSON(r, &req); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	capture, err := s.fetchCapture(r, req)
	if err != nil {
		writeExternalError(w, err, "api.capture_external_source")
		return
	}
	snapshot, _, err := s.externalStore.CaptureIssue(r.Context(), capture)
	if err != nil {
		s.writeInternalError(w, "capture external source failed")
		return
	}
	writeJSON(w, http.StatusCreated, gen.CaptureResult{SourceId: snapshot.SourceID, SnapshotId: snapshot.SnapshotID})
}
func (s *Server) handleGetExternalSourceSnapshot(w http.ResponseWriter, r *http.Request) {
	snapshot, _, err := s.externalStore.GetSnapshot(r.Context(), r.PathValue("id"))
	if err != nil {
		writeExternalError(w, err, "api.get_external_snapshot")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshot_id": snapshot.SnapshotID, "source_id": snapshot.SourceID, "issue": snapshot.Issue, "profile": snapshot.Profile, "provider_updated_at": snapshot.ProviderUpdatedAt, "fetched_at": snapshot.FetchedAt, "comment_count": snapshot.ProviderCommentCount, "fetched_comment_count": snapshot.FetchedCommentCount, "fetched_comment_pages": snapshot.FetchedCommentPages, "omitted_comment_count": snapshot.OmittedCommentCount, "omitted_comment_pages": snapshot.OmittedCommentPages, "retained_comment_count": snapshot.RetainedCommentCount, "retained_comment_bytes": snapshot.RetainedCommentBytes, "comments_truncated": snapshot.CommentsTruncated})
}

func (s *Server) handleGetExternalSourceSnapshotComments(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	start := 0
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		payload, decodeErr := s.cursors.Decode("external-comments", id, raw)
		if decodeErr != nil {
			s.writeBadRequest(w, "invalid comments cursor")
			return
		}
		start, _ = strconv.Atoi(payload)
	}
	comments, hasMore, err := s.externalStore.ListSnapshotComments(r.Context(), id, start, 50)
	if err != nil {
		writeExternalError(w, err, "api.get_external_snapshot_comments")
		return
	}
	response := map[string]any{"items": comments}
	if hasMore {
		response["next_cursor"] = s.cursors.Encode("external-comments", id, strconv.Itoa(start+len(comments)))
	}
	writeJSON(w, http.StatusOK, response)
}
func (s *Server) handleGetExternalSourceLinkHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	start := 0
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		payload, decodeErr := s.cursors.Decode("external-history", id, raw)
		if decodeErr != nil {
			s.writeBadRequest(w, "invalid history cursor")
			return
		}
		start, _ = strconv.Atoi(payload)
	}
	items, hasMore, err := s.externalStore.LinkHistoryPage(r.Context(), id, start, 50)
	if err != nil {
		writeExternalError(w, err, "api.get_external_link_history")
		return
	}
	response := map[string]any{"items": items}
	if hasMore {
		response["next_cursor"] = s.cursors.Encode("external-history", id, strconv.Itoa(start+len(items)))
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleRefreshExternalSourceLink(w http.ResponseWriter, r *http.Request) {
	basis, err := s.externalStore.GetRefreshBasis(r.Context(), r.PathValue("id"))
	if err != nil {
		writeExternalError(w, err, "api.refresh_external_link")
		return
	}
	started := s.now().UTC().Format(time.RFC3339Nano)
	request := state.ApplyExternalRefreshRequest{Basis: basis, ObservationID: s.idGenerator("external_observation"), AuditEventID: s.idGenerator("external_audit"), AliasID: s.idGenerator("external_alias"), ActorID: authenticatedActorID(r.Context()), StartedAt: started, FinishedAt: started}
	changes := gen.ExternalRefreshChanges{}
	excluded := false
	if basis.Connection.Lifecycle == "enabled" && basis.Connection.HasCurrentValidation() {
		if err := s.requireIncludedRepository(r.Context(), basis.Connection, basis.FetchRepositoryNativeID); err != nil {
			if !errors.Is(err, state.ErrExternalRepositoryExcluded) {
				writeExternalError(w, err, "api.refresh_external_link")
				return
			}
			excluded = true
		}
	}
	if basis.Connection.Lifecycle != "enabled" || !basis.Connection.HasCurrentValidation() {
		request.Outcome = "connection_disabled"
	} else if excluded {
		// The connection no longer includes this repository, so the source keeps
		// the content it already has and no provider call is made.
		request.Outcome = "repository_excluded"
	} else {
		adapter, ok := s.externalRegistry.Adapter(basis.Connection.Provider)
		if !ok {
			request.Outcome = "service_unavailable"
		} else {
			_, oldComments, _ := s.externalStore.GetSnapshot(r.Context(), basis.LatestSnapshot.SnapshotID)
			result, fetchErr := adapter.FetchIssue(r.Context(), external.IssueRequest{Connection: basis.Connection, RepositoryID: basis.FetchRepositoryNativeID, IssueNumber: basis.FetchIssueNumber, Profile: basis.LatestSnapshot.Profile, ETag: basis.LatestSnapshot.ETag, CommentsETag: basis.LatestSnapshot.CommentsETag, ProviderNativeID: basis.ProviderNativeID, CurrentIssue: basis.LatestSnapshot.Issue, CurrentCommentCount: basis.LatestSnapshot.ProviderCommentCount})
			request.FinishedAt = s.now().UTC().Format(time.RFC3339Nano)
			if fetchErr != nil {
				request.Outcome = refreshOutcome(fetchErr)
				var pe *external.ProviderError
				if errors.As(fetchErr, &pe) && pe.RetryAt != nil {
					request.RetryAt = pe.RetryAt.UTC().Format(time.RFC3339)
				}
			} else if !result.NotModified {
				if result.CommentsNotModified {
					result.Comments = oldComments
					result.FetchedCommentCount = basis.LatestSnapshot.FetchedCommentCount
					result.FetchedCommentPages = basis.LatestSnapshot.FetchedCommentPages
					result.OmittedCommentCount = result.ProviderCommentCount - result.FetchedCommentCount
					if result.OmittedCommentCount < 0 {
						result.OmittedCommentCount = 0
					}
					result.OmittedCommentPages = (result.OmittedCommentCount + 99) / 100
					result.CommentsETag = basis.LatestSnapshot.CommentsETag
				}
				capture := state.CaptureExternalIssueRequest{SourceID: basis.Link.SourceID, SnapshotID: s.idGenerator("external_snapshot"), ConnectionID: basis.Connection.ConnectionID, ProviderNativeID: basis.ProviderNativeID, RepositoryNativeID: result.RepositoryID, Issue: result.Issue, Profile: result.Profile, ProviderUpdatedAt: result.Issue.UpdatedAt, FetchedAt: request.FinishedAt, ETag: result.ETag, CommentsETag: result.CommentsETag, LastModified: result.LastModified, ProviderCommentCount: result.ProviderCommentCount, FetchedCommentCount: result.FetchedCommentCount, FetchedCommentPages: result.FetchedCommentPages, OmittedCommentCount: result.OmittedCommentCount, OmittedCommentPages: result.OmittedCommentPages, Comments: result.Comments}
				changes = compareExternalResult(basis.LatestSnapshot, oldComments, result)
				if !result.Transferred && result.RepositoryID == basis.FetchRepositoryNativeID && sameExternalResult(basis.LatestSnapshot, oldComments, result) {
					request.Outcome = "unchanged"
				} else {
					request.Capture = &capture
					request.Outcome = "changed"
				}
			} else {
				request.Outcome = "unchanged"
			}
		}
	}
	head, created, outcome, err := s.externalStore.ApplyRefresh(r.Context(), request)
	if err != nil {
		s.writeInternalError(w, "apply external refresh failed")
		return
	}
	response := map[string]any{"link": head, "outcome": outcome, "snapshot_created": created, "changes": changes, "message": refreshMessage(outcome, head)}
	if request.RetryAt != "" {
		response["retry_at"] = request.RetryAt
	}
	writeJSON(w, http.StatusOK, response)
}
func (s *Server) handleAdoptExternalSourceLink(w http.ResponseWriter, r *http.Request) {
	now := s.now().UTC().Format(time.RFC3339Nano)
	if err := s.externalStore.AdoptLatest(r.Context(), r.PathValue("id"), s.idGenerator("external_adoption"), authenticatedActorID(r.Context()), now, s.idGenerator("external_audit")); err != nil {
		writeExternalError(w, err, "api.adopt_external_link")
		return
	}
	writeJSON(w, http.StatusCreated, gen.ExternalAdoptionResult{Status: gen.ExternalAdoptionResultStatus("adopted")})
}

func (s *Server) handleDetachExternalSourceLink(w http.ResponseWriter, r *http.Request) {
	err := s.externalStore.DetachPlanningExternalSource(r.Context(), state.DetachPlanningExternalSourceRequest{
		LinkID:       r.PathValue("id"),
		ChangeID:     s.idGenerator("external_attachment_change"),
		ActorID:      authenticatedActorID(r.Context()),
		DetachedAt:   s.now().UTC().Format(time.RFC3339Nano),
		AuditEventID: s.idGenerator("external_audit"),
	})
	switch {
	case err == nil:
		writeJSON(w, http.StatusCreated, gen.ExternalDetachmentResult{Status: gen.ExternalDetachmentResultStatus("detached")})
	case errors.Is(err, state.ErrExternalDetachUnsupported):
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, err.Error(), "api.detach_external_link", map[string]any{"link_id": r.PathValue("id")})
	case errors.Is(err, state.ErrExternalSourceNotAttached):
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, err.Error(), "api.detach_external_link", map[string]any{"link_id": r.PathValue("id")})
	case errors.Is(err, state.ErrExternalPlanningSessionInactive):
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, err.Error(), "api.detach_external_link", map[string]any{"link_id": r.PathValue("id")})
	default:
		writeExternalError(w, err, "api.detach_external_link")
	}
}

func presentIssueResult(v external.IssueResult) map[string]any {
	comments, _, _ := state.NormalizeExternalComments(v.Comments)
	if comments == nil {
		comments = []state.ExternalComment{}
	}
	return map[string]any{"native_id": v.NativeID, "repository_id": v.RepositoryID, "issue": v.Issue, "profile": v.Profile, "comment_count": v.ProviderCommentCount, "comments": comments, "fetched_comment_count": v.FetchedCommentCount, "fetched_comment_pages": v.FetchedCommentPages, "omitted_comment_count": v.OmittedCommentCount, "omitted_comment_pages": v.OmittedCommentPages, "etag_present": v.ETag != ""}
}
func providerFailure(err error) string {
	var pe *external.ProviderError
	if errors.As(err, &pe) {
		return string(pe.Class)
	}
	return "service_unavailable"
}
func providerHealth(err error) string {
	switch providerFailure(err) {
	case "permission_mismatch":
		return "permission_mismatch"
	case "revoked":
		return "revoked"
	case "rate_limited":
		return "rate_limited"
	default:
		return "unavailable"
	}
}
func refreshOutcome(err error) string {
	switch providerFailure(err) {
	case "inaccessible_or_missing", "deleted", "rate_limited", "revoked", "permission_mismatch", "timeout", "malformed_response", "oversized_response":
		return providerFailure(err)
	default:
		return "service_unavailable"
	}
}

func compareExternalResult(old state.ExternalSourceSnapshot, oldComments []state.ExternalComment, v external.IssueResult) gen.ExternalRefreshChanges {
	changes := gen.ExternalRefreshChanges{
		Title: old.Issue.Title != v.Issue.Title, Body: old.Issue.Body != v.Issue.Body,
		State: old.Issue.State != v.Issue.State, Repository: old.Issue.Repository != v.Issue.Repository,
		IssueNumber: old.Issue.IssueNumber != v.Issue.IssueNumber, CanonicalUrl: old.Issue.CanonicalURL != v.Issue.CanonicalURL,
		CreatedAt: old.Issue.CreatedAt != v.Issue.CreatedAt, UpdatedAt: old.Issue.UpdatedAt != v.Issue.UpdatedAt,
		CommentCount: old.ProviderCommentCount != v.ProviderCommentCount,
	}
	if v.Profile.IncludeComments {
		normalized, _, _ := state.NormalizeExternalComments(v.Comments)
		a, _ := json.Marshal(oldComments)
		b, _ := json.Marshal(normalized)
		changes.Comments = string(a) != string(b)
		changes.Comments = changes.Comments || old.FetchedCommentCount != v.FetchedCommentCount || old.FetchedCommentPages != v.FetchedCommentPages || old.OmittedCommentCount != v.OmittedCommentCount || old.OmittedCommentPages != v.OmittedCommentPages
	}
	return changes
}
func sameExternalResult(old state.ExternalSourceSnapshot, oldComments []state.ExternalComment, v external.IssueResult) bool {
	a, _ := json.Marshal(old.Issue)
	b, _ := json.Marshal(v.Issue)
	pa, _ := json.Marshal(old.Profile)
	pb, _ := json.Marshal(v.Profile)
	if string(a) != string(b) || string(pa) != string(pb) || old.ProviderCommentCount != v.ProviderCommentCount || old.FetchedCommentCount != v.FetchedCommentCount || old.FetchedCommentPages != v.FetchedCommentPages || old.OmittedCommentCount != v.OmittedCommentCount || old.OmittedCommentPages != v.OmittedCommentPages {
		return false
	}
	if !v.Profile.IncludeComments {
		return true
	}
	type comparableComment struct{ ID, Author, Body, URL, CreatedAt, UpdatedAt string }
	comparable := func(items []state.ExternalComment) []comparableComment {
		out := make([]comparableComment, 0, len(items))
		for _, c := range items {
			out = append(out, comparableComment{c.ProviderCommentID, c.Author, c.Body, c.CanonicalURL, c.CreatedAt, c.UpdatedAt})
		}
		return out
	}
	normalized, _, _ := state.NormalizeExternalComments(v.Comments)
	oldRaw, _ := json.Marshal(comparable(oldComments))
	newRaw, _ := json.Marshal(comparable(normalized))
	return string(oldRaw) == string(newRaw)
}
func refreshMessage(outcome string, v state.ExternalLinkSummary) string {
	if v.TargetKind == "decree" && outcome == "changed" {
		return "The GitHub source changed; this decree was not changed."
	}
	switch outcome {
	case "changed":
		return "A newer GitHub source version is available."
	case "unchanged":
		return "The GitHub source is unchanged."
	case "superseded":
		return "Another refresh finished first; current source status is shown."
	case "revoked":
		return "GitHub App access was revoked or is invalid. Restore the installation, then run the connection health check."
	case "permission_mismatch":
		return "GitHub App configuration no longer matches. Configure exactly repository metadata read and Issues read, with zero subscribed events and no additional permissions, then run the connection health check."
	case "timeout":
		return "The GitHub refresh timed out. Check connectivity and retry."
	case "rate_limited":
		return "GitHub rate limited the refresh. Retry after the reported reset time."
	case "inaccessible_or_missing":
		return "The GitHub issue is inaccessible or missing. Check installation repository access."
	case "deleted":
		return "The linked GitHub issue was deleted. Historical Kingdom provenance remains available."
	case "repository_excluded":
		return "This repository is not included in the GitHub connection, so the source was left as it is. Include the repository to resume refreshing."
	case "connection_disabled":
		return "The GitHub connection is disabled. Enable and health-check it before refreshing."
	case "malformed_response":
		return "GitHub returned an invalid response. Retry later and check connection health if the problem continues."
	case "oversized_response":
		return "The GitHub response exceeded Kingdom's safe size limit. Reduce the source content before retrying."
	default:
		return "GitHub is currently unavailable. Retry later."
	}
}
func writeExternalError(w http.ResponseWriter, err error, op string) {
	status := http.StatusBadGateway
	code := errorcatalog.CodeDependencyBlocked
	message := "external provider request failed"
	if errors.Is(err, state.ErrExternalNotFound) {
		status = http.StatusNotFound
		message = "external resource not found"
	}
	if errors.Is(err, state.ErrExternalConflict) {
		status = http.StatusConflict
		message = "external resource changed concurrently; retry from current state"
	}
	if errors.Is(err, state.ErrExternalValidationNeeded) {
		status = http.StatusConflict
		message = "GitHub connection must pass a health check for its current credential before use"
	}
	if errors.Is(err, state.ErrExternalConnectionUnavailable) {
		status = http.StatusConflict
		message = "GitHub connection is disabled; enable and health-check it before use"
	}
	if errors.Is(err, state.ErrExternalRepositoryExcluded) {
		status = http.StatusConflict
		message = "this repository is not included in the GitHub connection; include it to read from it"
	}
	var pe *external.ProviderError
	var retryAt *time.Time
	var details map[string]any
	if errors.As(err, &pe) {
		retryAt = pe.RetryAt
		details = map[string]any{"provider_failure_class": string(pe.Class)}
		switch pe.Class {
		case external.FailureRateLimited:
			status = http.StatusTooManyRequests
			message = "GitHub rate limit reached; retry after the reported reset"
		case external.FailureAuthentication:
			status = http.StatusFailedDependency
			message = "GitHub authorization is invalid or revoked; reconnect the connection before retrying"
		case external.FailurePermissionMismatch:
			status = http.StatusForbidden
			message = "GitHub App must have exactly metadata read and issues read, zero subscribed events, and no additional permissions"
		case external.FailureInaccessibleOrMissing:
			status = http.StatusNotFound
			message = "GitHub issue is inaccessible or missing"
		case external.FailureDeleted:
			status = http.StatusGone
			message = "GitHub issue was deleted"
		case external.FailureTimeout:
			status = http.StatusGatewayTimeout
			message = "GitHub request timed out"
		}
	}
	writeCanonicalErrorWithRetryAt(w, status, code, message, op, details, retryAt)
}
