package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/planning"
	"kingdom/internal/state"
)

func (s *Server) handleCreatePlanningSession(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.create_planning_session"); !ok {
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
	session, err := s.planningService.CreateSession(r.Context(), title, modelSelectionJSON)
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
	if optionalTrimmedString(session.CurrentQuestionMessageID) != nil {
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
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.list_planning_sessions"); !ok {
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
		response.Items = append(response.Items, planningSessionSummaryFromState(item))
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleGetPlanningSession(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.get_planning_session"); !ok {
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
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.update_planning_session"); !ok {
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
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.delete_planning_session"); !ok {
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

func (s *Server) handleListPlanningSessionMessages(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.list_planning_session_messages"); !ok {
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
	variantCounts, err := s.planningStore.VariantCountsBySession(r.Context(), planningSessionID)
	if err != nil {
		s.writeInternalError(w, "fetch variant counts failed")
		return
	}
	for _, item := range page.Items {
		msg, presentErr := planningMessageFromState(item)
		if presentErr != nil {
			s.writeInternalError(w, "decode planning session messages failed")
			return
		}
		if count, ok := variantCounts[item.TurnIndex]; ok && item.Role == state.PlanningMessageRoleAssistant {
			msg.VariantCount = &count
		}
		response.Items = append(response.Items, msg)
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleGetPlanningSessionPlan(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.get_planning_session_plan"); !ok {
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
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.activate_planning_message_variant"); !ok {
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
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.list_planning_message_variants"); !ok {
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
	message, err := s.planningService.GetMessageInSession(r.Context(), planningSessionID, messageID)
	if err != nil {
		handlePlanningError(w, err, "api.list_planning_message_variants")
		return
	}
	variants, err := s.planningService.ListMessageVariants(r.Context(), planningSessionID, message.TurnIndex, message.Role)
	if err != nil {
		s.writeInternalError(w, "list planning message variants failed")
		return
	}
	response := gen.PlanningMessageVariantsResponse{Items: make([]gen.PlanningMessage, 0, len(variants))}
	for _, v := range variants {
		presented, presentErr := planningMessageFromState(v)
		if presentErr != nil {
			s.writeInternalError(w, "decode planning message variants failed")
			return
		}
		response.Items = append(response.Items, presented)
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleCreatePlanningSessionOperation(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.create_planning_session_operation"); !ok {
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

	var req gen.CreatePlanningOperationRequest
	if err := decodeStrictJSON(r, &req); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}

	input := planning.CreateOperationInput{Type: state.PlanningOperationType(strings.TrimSpace(string(req.Type)))}
	if req.Reply != nil {
		input.ReplyMarkdown = strings.TrimSpace(req.Reply.ContentMarkdown)
		if req.Reply.AnswerPayload != nil {
			raw, err := json.Marshal(req.Reply.AnswerPayload)
			if err != nil {
				s.writeInternalError(w, "encode planning reply answer payload failed")
				return
			}
			input.AnswerPayloadJSON = string(raw)
		}
		if req.Reply.TruncateFromTurn != nil {
			v := int(*req.Reply.TruncateFromTurn)
			input.TruncateFromTurn = &v
		}
	}
	if req.Regenerate != nil {
		input.TargetTurnIndex = int(req.Regenerate.TargetTurnIndex)
	}

	result, err := s.planningService.CreateOperation(r.Context(), planningSessionID, input)
	if err != nil {
		handlePlanningError(w, err, "api.create_planning_session_operation")
		return
	}
	_, _, request, err := s.planningService.GetOperation(r.Context(), result.Operation.OperationID)
	if err != nil {
		s.writeInternalError(w, "fetch planning operation failed")
		return
	}
	autoTitleEligible, err := s.presentPlanningSessionAutoTitleEligible(r.Context(), result.Session)
	if err != nil {
		s.writeInternalError(w, "evaluate planning session title eligibility failed")
		return
	}
	writeJSON(w, http.StatusCreated, createPlanningOperationResultFromState(result, request, autoTitleEligible))
}

func (s *Server) handleGetPlanningOperation(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.get_planning_operation"); !ok {
		return
	}
	if s.planningService == nil {
		s.writeInternalError(w, "planning service is not configured")
		return
	}
	operationID := strings.TrimSpace(r.PathValue("id"))
	if operationID == "" {
		s.writeBadRequest(w, "planning operation id is required")
		return
	}
	operation, ok, request, err := s.planningService.GetOperation(r.Context(), operationID)
	if err != nil {
		s.writeInternalError(w, "fetch planning operation failed")
		return
	}
	if !ok {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "planning operation not found", "api.get_planning_operation", map[string]any{"operation_id": operationID})
		return
	}
	writeJSON(w, http.StatusOK, planningOperationDetailFromState(operation, request))
}

func (s *Server) handleRefreshPlanningSessionContext(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.refresh_planning_session_context"); !ok {
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

func (s *Server) handleGeneratePlanningSessionTitle(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.generate_planning_session_title"); !ok {
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
	session, err := s.planningService.GenerateTitle(r.Context(), planningSessionID)
	if err != nil {
		handlePlanningError(w, err, "api.generate_planning_session_title")
		return
	}
	presentedSession, err := s.presentPlanningSessionDetail(r.Context(), session)
	if err != nil {
		s.writeInternalError(w, "evaluate planning session title eligibility failed")
		return
	}
	writeJSON(w, http.StatusOK, presentedSession)
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
	return planningSessionDetailFromState(session, autoTitleEligible), nil
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
