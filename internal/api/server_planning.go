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

	openapi_types "github.com/oapi-codegen/runtime/types"
	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/planning"
	"kingdom/internal/state"
)

func (s *Server) handleCreatePlanningSession(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.create_planning_session"); !ok {
		return
	}
	if s.planningService == nil {
		s.writeInternalError(w, "planning service is not configured")
		return
	}

	var req gen.CreatePlanningSessionRequest
	if err := decodeStrictJSON(r, &req); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}

	modelSelectionJSON := `{}`
	if req.ModelSelection != nil {
		raw, err := json.Marshal(req.ModelSelection)
		if err != nil {
			s.writeInternalError(w, "encode planning model selection failed")
			return
		}
		modelSelectionJSON = string(raw)
	}

	title := ""
	if req.Title != nil {
		title = *req.Title
	}
	var session state.PlanningSession
	var err error
	if req.ExternalIntake != nil {
		capture, captureErr := s.fetchCapture(r, *req.ExternalIntake)
		if captureErr != nil {
			writeExternalError(w, captureErr, "api.create_planning_session")
			return
		}
		if strings.TrimSpace(title) == "" {
			title = capture.Issue.Title
		}
		session, err = s.planningService.CreateSessionFromExternalIssue(r.Context(), title, modelSelectionJSON, capture, s.idGenerator("external_link"), s.idGenerator("external_adoption"), authenticatedActorID(r.Context()))
	} else {
		session, err = s.planningService.CreateSession(r.Context(), title, modelSelectionJSON)
	}
	if err != nil {
		handlePlanningError(w, err, "api.create_planning_session")
		return
	}

	presentedSession, err := s.presentPlanningSessionDetail(r.Context(), session)
	if err != nil {
		s.writeInternalError(w, "evaluate planning session title eligibility failed")
		return
	}
	result := gen.CreatePlanningSessionResult{Session: presentedSession}
	if optionalTrimmedString(session.ActiveLeafMessageID) != nil {
		messages, listErr := s.planningStore.ListMessagesPage(r.Context(), session.PlanningSessionID, 1, nil)
		if listErr != nil {
			s.writeInternalError(w, "list planning session messages failed")
			return
		}
		if len(messages.Items) > 0 {
			opening, presentErr := planningMessageFromState(messages.Items[0])
			if presentErr != nil {
				s.writeInternalError(w, "decode planning session messages failed")
				return
			}
			result.OpeningMessage = &opening
		}
	}

	writeJSON(w, http.StatusCreated, result)
}

func (s *Server) handleListPlanningSessions(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.list_planning_sessions"); !ok {
		return
	}
	limit, err := parseLimitQuery(r.URL.Query().Get("limit"), 100)
	if err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	items, err := s.planningStore.ListSessions(r.Context(), limit)
	if err != nil {
		s.writeInternalError(w, "list planning sessions failed")
		return
	}
	response := gen.PlanningSessionListResponse{Items: make([]gen.PlanningSessionSummary, 0, len(items))}
	for _, item := range items {
		presentedRuns, resolveErr := s.presentPlanningSessionRuns(r.Context(), item)
		if resolveErr != nil {
			s.writeInternalError(w, "evaluate planning session run state failed")
			return
		}
		response.Items = append(response.Items, planningSessionSummaryFromState(item, presentedRuns))
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleGetPlanningSession(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.get_planning_session"); !ok {
		return
	}
	planningSessionID := strings.TrimSpace(r.PathValue("id"))
	if planningSessionID == "" {
		s.writeBadRequest(w, "planning session id is required")
		return
	}
	session, ok, err := s.planningStore.GetSession(r.Context(), planningSessionID)
	if err != nil {
		s.writeInternalError(w, "fetch planning session failed")
		return
	}
	if !ok {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "planning session not found", "api.get_planning_session", map[string]any{"planning_session_id": planningSessionID})
		return
	}
	presentedSession, err := s.presentPlanningSessionDetail(r.Context(), session)
	if err != nil {
		s.writeInternalError(w, "evaluate planning session title eligibility failed")
		return
	}
	writeJSON(w, http.StatusOK, presentedSession)
}

func (s *Server) handleUpdatePlanningSession(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.update_planning_session"); !ok {
		return
	}
	if s.planningService == nil {
		s.writeInternalError(w, "planning service is not configured")
		return
	}
	planningSessionID := strings.TrimSpace(r.PathValue("id"))
	if planningSessionID == "" {
		s.writeBadRequest(w, "planning session id is required")
		return
	}

	var req gen.UpdatePlanningSessionRequest
	if err := decodeStrictJSON(r, &req); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}

	input := planning.UpdateSessionInput{Title: req.Title}
	if req.ModelSelection != nil {
		raw, err := json.Marshal(req.ModelSelection)
		if err != nil {
			s.writeInternalError(w, "encode planning model selection failed")
			return
		}
		modelSelectionJSON := string(raw)
		input.ModelSelectionJSON = &modelSelectionJSON
	}

	updated, err := s.planningService.UpdateSession(r.Context(), planningSessionID, input)
	if err != nil {
		handlePlanningError(w, err, "api.update_planning_session")
		return
	}

	presentedSession, err := s.presentPlanningSessionDetail(r.Context(), updated)
	if err != nil {
		s.writeInternalError(w, "evaluate planning session title eligibility failed")
		return
	}
	writeJSON(w, http.StatusOK, presentedSession)
}

func (s *Server) handleDeletePlanningSession(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.delete_planning_session"); !ok {
		return
	}
	planningSessionID := strings.TrimSpace(r.PathValue("id"))
	if planningSessionID == "" {
		s.writeBadRequest(w, "planning session id is required")
		return
	}
	deleted, err := s.planningService.DeleteSession(r.Context(), planningSessionID)
	if err != nil {
		handlePlanningError(w, err, "api.delete_planning_session")
		return
	}
	if !deleted {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "planning session not found", "api.delete_planning_session", map[string]any{"planning_session_id": planningSessionID})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleAttachPlanningExternalSource(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.attach_planning_external_source"); !ok {
		return
	}
	planningSessionID := strings.TrimSpace(r.PathValue("id"))
	if planningSessionID == "" {
		s.writeBadRequest(w, "planning session id is required")
		return
	}
	var req gen.ExternalIntake
	if err := decodeStrictJSON(r, &req); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}

	// Provider I/O must finish before the short provenance transaction.
	capture, err := s.fetchCapture(r, req)
	if err != nil {
		writeExternalError(w, err, "api.attach_planning_external_source")
		return
	}
	at := s.now().UTC().Format(time.RFC3339Nano)
	link, err := s.externalStore.AttachPlanningExternalSource(r.Context(), state.AttachPlanningExternalSourceRequest{
		PlanningSessionID: planningSessionID,
		Capture:           capture,
		LinkID:            s.idGenerator("external_link"),
		AdoptionID:        s.idGenerator("external_adoption"),
		AuditEventID:      s.idGenerator("external_audit"),
		ActorID:           authenticatedActorID(r.Context()),
		AttachedAt:        at,
	})
	if err != nil {
		switch {
		case errors.Is(err, state.ErrExternalPlanningSessionNotFound):
			writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "planning session not found", "api.attach_planning_external_source", map[string]any{"planning_session_id": planningSessionID})
		case errors.Is(err, state.ErrExternalPlanningSessionInactive):
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, err.Error(), "api.attach_planning_external_source", map[string]any{"planning_session_id": planningSessionID})
		case errors.Is(err, state.ErrExternalSourceAlreadyAttached):
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, err.Error(), "api.attach_planning_external_source", map[string]any{"planning_session_id": planningSessionID})
		case errors.Is(err, state.ErrExternalConflict):
			writeExternalError(w, err, "api.attach_planning_external_source")
		default:
			s.writeInternalError(w, "attach planning external source failed")
		}
		return
	}
	writeJSON(w, http.StatusCreated, externalProvenanceFromState(link, true))
}

func (s *Server) handleListPlanningSessionMessages(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.list_planning_session_messages"); !ok {
		return
	}
	planningSessionID := strings.TrimSpace(r.PathValue("id"))
	if planningSessionID == "" {
		s.writeBadRequest(w, "planning session id is required")
		return
	}
	if _, ok, err := s.planningStore.GetSession(r.Context(), planningSessionID); err != nil {
		s.writeInternalError(w, "fetch planning session failed")
		return
	} else if !ok {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "planning session not found", "api.list_planning_session_messages", map[string]any{"planning_session_id": planningSessionID})
		return
	}
	limit, err := parseLimitQuery(r.URL.Query().Get("limit"), 50)
	if err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	var before *int
	if rawBefore := strings.TrimSpace(r.URL.Query().Get("before")); rawBefore != "" {
		parsedBefore, beforeErr := s.decodePlanningMessagesCursor(planningSessionID, rawBefore)
		if beforeErr != nil {
			s.writeBadRequest(w, "invalid before cursor")
			return
		}
		before = &parsedBefore
	}
	page, err := s.planningStore.ListMessagesPage(r.Context(), planningSessionID, limit, before)
	if err != nil {
		s.writeInternalError(w, "list planning session messages failed")
		return
	}
	response := gen.PlanningMessagesPage{
		PlanningSessionId: planningSessionID,
		Items:             make([]gen.PlanningMessage, 0, len(page.Items)),
		TotalCount:        page.TotalCount,
		FirstItemIndex:    page.FirstItemIndex,
	}
	if page.FirstItemIndex > 0 {
		previousCursor := s.encodePlanningMessagesCursor(planningSessionID, page.FirstItemIndex)
		response.PreviousCursor = &previousCursor
	}
	branchInfo, err := s.planningStore.BranchInfoBySession(r.Context(), planningSessionID)
	if err != nil {
		s.writeInternalError(w, "fetch branch info failed")
		return
	}
	messageIDs := make([]string, 0, len(page.Items))
	for _, item := range page.Items {
		messageIDs = append(messageIDs, item.PlanningMessageID)
	}
	messageRunIDs, err := s.planningMessageRunIDs(r.Context(), planningSessionID, messageIDs)
	if err != nil {
		s.writeInternalError(w, "fetch planning message activity linkage failed")
		return
	}
	for _, item := range page.Items {
		msg, presentErr := planningMessageFromState(item)
		if presentErr != nil {
			s.writeInternalError(w, "decode planning session messages failed")
			return
		}
		if branch, ok := branchInfo[item.PlanningMessageID]; ok {
			index, count := branch.Index, branch.Count
			msg.BranchIndex = &index
			msg.BranchCount = &count
		}
		if runID, ok := messageRunIDs[item.PlanningMessageID]; ok {
			msg.SourceRunId = &runID
		}
		response.Items = append(response.Items, msg)
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleGetPlanningSessionPlan(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.get_planning_session_plan"); !ok {
		return
	}
	planningSessionID := strings.TrimSpace(r.PathValue("id"))
	if planningSessionID == "" {
		s.writeBadRequest(w, "planning session id is required")
		return
	}
	draft, ok, err := s.planningStore.GetCurrentDraft(r.Context(), planningSessionID)
	if err != nil {
		s.writeInternalError(w, "fetch planning session plan failed")
		return
	}
	if !ok {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "planning session plan not found", "api.get_planning_session_plan", map[string]any{"planning_session_id": planningSessionID})
		return
	}
	writeJSON(w, http.StatusOK, planningDraftFromState(draft))
}

func (s *Server) handleActivatePlanningMessageVariant(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.activate_planning_message_variant"); !ok {
		return
	}
	if s.planningService == nil {
		s.writeInternalError(w, "planning service is not configured")
		return
	}
	planningSessionID := strings.TrimSpace(r.PathValue("id"))
	if planningSessionID == "" {
		s.writeBadRequest(w, "planning session id is required")
		return
	}
	messageID := strings.TrimSpace(r.PathValue("messageId"))
	if messageID == "" {
		s.writeBadRequest(w, "message id is required")
		return
	}
	activated, err := s.planningService.ActivateVariant(r.Context(), planningSessionID, messageID)
	if err != nil {
		handlePlanningError(w, err, "api.activate_planning_message_variant")
		return
	}
	presented, presentErr := planningMessageFromState(activated)
	if presentErr != nil {
		s.writeInternalError(w, "decode planning message failed")
		return
	}
	writeJSON(w, http.StatusOK, presented)
}

func (s *Server) handleListPlanningMessageVariants(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.list_planning_message_variants"); !ok {
		return
	}
	if s.planningService == nil {
		s.writeInternalError(w, "planning service is not configured")
		return
	}
	planningSessionID := strings.TrimSpace(r.PathValue("id"))
	if planningSessionID == "" {
		s.writeBadRequest(w, "planning session id is required")
		return
	}
	messageID := strings.TrimSpace(r.PathValue("messageId"))
	if messageID == "" {
		s.writeBadRequest(w, "message id is required")
		return
	}
	if _, err := s.planningService.GetMessageInSession(r.Context(), planningSessionID, messageID); err != nil {
		handlePlanningError(w, err, "api.list_planning_message_variants")
		return
	}
	variants, err := s.planningService.ListMessageBranches(r.Context(), planningSessionID, messageID)
	if err != nil {
		s.writeInternalError(w, "list planning message branches failed")
		return
	}
	response := gen.PlanningMessageVariantsResponse{Items: make([]gen.PlanningMessage, 0, len(variants))}
	messageIDs := make([]string, 0, len(variants))
	for _, item := range variants {
		messageIDs = append(messageIDs, item.PlanningMessageID)
	}
	messageRunIDs, err := s.planningMessageRunIDs(r.Context(), planningSessionID, messageIDs)
	if err != nil {
		s.writeInternalError(w, "fetch planning message activity linkage failed")
		return
	}
	for _, v := range variants {
		presented, presentErr := planningMessageFromState(v)
		if presentErr != nil {
			s.writeInternalError(w, "decode planning message variants failed")
			return
		}
		if runID, ok := messageRunIDs[v.PlanningMessageID]; ok {
			presented.SourceRunId = &runID
		}
		response.Items = append(response.Items, presented)
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) planningMessageRunIDs(ctx context.Context, planningSessionID string, messageIDs []string) (map[string]openapi_types.UUID, error) {
	operations, err := s.planningStore.ListLatestOperationsByResultRefs(ctx, planningSessionID, messageIDs)
	if err != nil {
		return nil, err
	}
	result := make(map[string]openapi_types.UUID)
	for _, operation := range operations {
		if operation.ResultRef == nil || strings.TrimSpace(*operation.ResultRef) == "" || strings.TrimSpace(operation.AcpRunID) == "" {
			continue
		}
		result[strings.TrimSpace(*operation.ResultRef)] = openapi_types.UUID(mustParseOpenAPIUUID(operation.AcpRunID))
	}
	return result, nil
}

func (s *Server) handleRefreshPlanningSessionContext(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.refresh_planning_session_context"); !ok {
		return
	}
	if s.planningService == nil {
		s.writeInternalError(w, "planning service is not configured")
		return
	}
	planningSessionID := strings.TrimSpace(r.PathValue("id"))
	if planningSessionID == "" {
		s.writeBadRequest(w, "planning session id is required")
		return
	}
	// The request body for /context/refresh used to decode an empty schema and
	// silently swallow EOFs. We dropped the schema in OpenAPI 0.2.0 — the
	// endpoint is parameterless. Any client still sending a body is harmless;
	// we ignore it.
	snapshot, session, err := s.planningService.RefreshContext(r.Context(), planningSessionID)
	if err != nil {
		handlePlanningError(w, err, "api.refresh_planning_session_context")
		return
	}
	presentedSession, err := s.presentPlanningSessionDetail(r.Context(), session)
	if err != nil {
		s.writeInternalError(w, "evaluate planning session title eligibility failed")
		return
	}
	writeJSON(w, http.StatusOK, gen.RefreshPlanningSessionContextResult{
		Session:         presentedSession,
		ContextSnapshot: planningContextSnapshotFromState(snapshot),
	})
}

func (s *Server) presentPlanningSessionAutoTitleEligible(ctx context.Context, session state.PlanningSession) (bool, error) {
	if s == nil || s.planningService == nil {
		return false, nil
	}
	return s.planningService.AutoTitleEligible(ctx, session)
}

func (s *Server) presentPlanningSessionDetail(ctx context.Context, session state.PlanningSession) (gen.PlanningSessionDetail, error) {
	autoTitleEligible, err := s.presentPlanningSessionAutoTitleEligible(ctx, session)
	if err != nil {
		return gen.PlanningSessionDetail{}, err
	}
	presentedRuns, err := s.presentPlanningSessionRuns(ctx, session)
	if err != nil {
		return gen.PlanningSessionDetail{}, err
	}
	result := planningSessionDetailFromState(session, autoTitleEligible, presentedRuns)
	links, linkErr := s.externalStore.GetLinksForTarget(ctx, "planning_session", session.PlanningSessionID)
	if linkErr != nil {
		return gen.PlanningSessionDetail{}, linkErr
	}
	result.ExternalSources = externalProvenanceListFromState(links, true)
	return result, nil
}

func (s *Server) presentPlanningSessionRuns(ctx context.Context, session state.PlanningSession) (planningSessionPresentedRuns, error) {
	var interactiveOperation *state.PlanningOperation
	if session.CurrentOperationID != nil {
		operation, ok, err := s.planningStore.GetOperation(ctx, *session.CurrentOperationID)
		if err != nil {
			return planningSessionPresentedRuns{}, err
		}
		if ok {
			interactiveOperation = &operation
		}
	}
	var titleOperation *state.PlanningOperation
	if session.CurrentTitleOperationID != nil {
		operation, ok, err := s.planningStore.GetOperation(ctx, *session.CurrentTitleOperationID)
		if err != nil {
			return planningSessionPresentedRuns{}, err
		}
		if ok {
			titleOperation = &operation
		}
	}
	return resolvePresentedPlanningSessionRuns(session, interactiveOperation, titleOperation), nil
}

func (s *Server) encodePlanningMessagesCursor(sessionID string, before int) string {
	return s.cursors.Encode("planning-messages", sessionID, strconv.Itoa(before))
}

func (s *Server) decodePlanningMessagesCursor(sessionID, cursor string) (int, error) {
	payload, err := s.cursors.Decode("planning-messages", sessionID, cursor)
	if err != nil {
		return 0, err
	}
	value, err := strconv.Atoi(strings.TrimSpace(payload))
	if err != nil || value < 0 {
		return 0, fmt.Errorf("invalid planning messages cursor")
	}
	return value, nil
}
