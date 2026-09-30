package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/mailboxview"
	"kingdom/internal/state"
)

type runMailboxMessagesResponse struct {
	RunID string                     `json:"run_id"`
	Items []runMailboxMessageSummary `json:"items"`
}

type runMailboxMessageSummary struct {
	MessageID         string  `json:"message_id"`
	RunID             string  `json:"run_id"`
	NodeID            string  `json:"node_id"`
	FromAgentID       string  `json:"from_agent_id"`
	FromAgentName     string  `json:"from_agent_name"`
	ToAgentID         string  `json:"to_agent_id"`
	ToAgentName       string  `json:"to_agent_name"`
	ThreadID          string  `json:"thread_id"`
	MessageType       string  `json:"message_type"`
	CommunicationType string  `json:"communication_type"`
	MessageSource     string  `json:"message_source"`
	ReasonCode        *string `json:"reason_code,omitempty"`
	VerdictType       *string `json:"verdict_type,omitempty"`
	VerdictStatus     *string `json:"verdict_status,omitempty"`
	Priority          string  `json:"priority"`
	CreatedAt         string  `json:"created_at"`
	ReadAt            *string `json:"read_at,omitempty"`
	Preview           string  `json:"preview"`
	Format            string  `json:"format"`
}

type runMailboxMessageDetail struct {
	MessageID         string  `json:"message_id"`
	RunID             string  `json:"run_id"`
	NodeID            string  `json:"node_id"`
	FromAgentID       string  `json:"from_agent_id"`
	FromAgentName     string  `json:"from_agent_name"`
	ToAgentID         string  `json:"to_agent_id"`
	ToAgentName       string  `json:"to_agent_name"`
	ThreadID          string  `json:"thread_id"`
	MessageType       string  `json:"message_type"`
	CommunicationType string  `json:"communication_type"`
	MessageSource     string  `json:"message_source"`
	ReasonCode        *string `json:"reason_code,omitempty"`
	VerdictType       *string `json:"verdict_type,omitempty"`
	VerdictStatus     *string `json:"verdict_status,omitempty"`
	Priority          string  `json:"priority"`
	CreatedAt         string  `json:"created_at"`
	ReadAt            *string `json:"read_at,omitempty"`
	Format            string  `json:"format"`
	Content           string  `json:"content"`
	PayloadJSON       any     `json:"payload_json"`
}

func (s *Server) handleListRunMailboxMessages(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.list_run_mailbox_messages"); !ok {
		return
	}

	runID := strings.TrimSpace(r.PathValue("id"))
	if runID == "" {
		s.writeBadRequest(w, "run id is required")
		return
	}
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.list_run_mailbox_messages", map[string]any{"run_id": runID})
		return
	}

	limit, err := parseMailboxLimit(r.URL.Query().Get("limit"))
	if err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	threadID := strings.TrimSpace(r.URL.Query().Get("thread_id"))
	toAgentID := strings.TrimSpace(r.URL.Query().Get("to_agent_id"))
	communicationType := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("communication_type")))
	unreadOnly, err := parseOptionalBool(r.URL.Query().Get("unread_only"))
	if err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}

	messages, err := s.listUserFacingRunMailboxMessages(r.Context(), runID, threadID, toAgentID, communicationType, unreadOnly, limit)
	if err != nil {
		s.writeInternalError(w, "list mailbox messages failed")
		return
	}

	items := make([]runMailboxMessageSummary, 0, len(messages))
	agentNames, err := s.lookupAgentDisplayNames(r.Context(), messages)
	if err != nil {
		s.writeInternalError(w, "resolve mailbox agent names failed")
		return
	}
	for _, message := range messages {
		payload, _, _, _ := parseMailboxPayload(message.PayloadJSON)
		resolvedType := mailboxCommunicationType(payload, message)
		if !isUserFacingMailboxMessage(message, payload, resolvedType) {
			continue
		}
		summary := summarizeMailboxMessage(message, agentNames)
		if communicationType != "" && strings.ToLower(strings.TrimSpace(summary.CommunicationType)) != communicationType {
			continue
		}
		items = append(items, summary)
	}

	writeJSON(w, http.StatusOK, runMailboxMessagesResponse{RunID: runID, Items: items})
}

func (s *Server) listUserFacingRunMailboxMessages(ctx context.Context, runID, threadID, toAgentID, communicationType string, unreadOnly bool, limit int) ([]state.MailboxMessage, error) {
	unreadMarker := 0
	if unreadOnly {
		unreadMarker = 1
	}

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
SELECT message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at
FROM mailbox_messages
WHERE run_id = ?
	  AND (? = '' OR thread_id = ?)
	  AND (? = '' OR to_agent_id = ?)
	  AND (? = 0 OR read_at IS NULL)
	  AND %s
	  AND (? = '' OR lower(COALESCE(json_extract(payload_json, '$.communication_type'), '')) = ?)
ORDER BY message_seq DESC
LIMIT ?`, mailboxview.UserFacingMailboxPredicate("")),
		runID,
		threadID,
		threadID,
		toAgentID,
		toAgentID,
		unreadMarker,
		communicationType,
		communicationType,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	messages := make([]state.MailboxMessage, 0, limit)
	for rows.Next() {
		var message state.MailboxMessage
		if scanErr := rows.Scan(
			&message.MessageID,
			&message.RunID,
			&message.FromAgentID,
			&message.ToAgentID,
			&message.ThreadID,
			&message.MessageType,
			&message.Priority,
			&message.PayloadJSON,
			&message.CreatedAt,
			&message.ReadAt,
		); scanErr != nil {
			return nil, scanErr
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return messages, nil
}

func (s *Server) handleGetRunMailboxMessage(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.get_run_mailbox_message"); !ok {
		return
	}

	runID := strings.TrimSpace(r.PathValue("id"))
	messageID := strings.TrimSpace(r.PathValue("messageId"))
	if runID == "" || messageID == "" {
		s.writeBadRequest(w, "run id and message id are required")
		return
	}
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.get_run_mailbox_message", map[string]any{"run_id": runID})
		return
	}

	message, found, err := s.loadMailboxMessageByID(r.Context(), runID, messageID)
	if err != nil {
		s.writeInternalError(w, "get mailbox message failed")
		return
	}
	if !found {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "mailbox message not found", "api.get_run_mailbox_message", map[string]any{"run_id": runID, "message_id": messageID})
		return
	}

	agentNames, err := s.lookupAgentDisplayNames(r.Context(), []state.MailboxMessage{message})
	if err != nil {
		s.writeInternalError(w, "resolve mailbox agent names failed")
		return
	}

	detail := detailMailboxMessage(message, agentNames)
	payload, _, _, _ := parseMailboxPayload(message.PayloadJSON)
	resolvedType := mailboxCommunicationType(payload, message)
	if !isUserFacingMailboxMessage(message, payload, resolvedType) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "mailbox message not found", "api.get_run_mailbox_message", map[string]any{"run_id": runID, "message_id": messageID})
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) handleMarkRunMailboxMessageRead(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.mark_run_mailbox_message_read"); !ok {
		return
	}

	runID := strings.TrimSpace(r.PathValue("id"))
	messageID := strings.TrimSpace(r.PathValue("messageId"))
	if runID == "" || messageID == "" {
		s.writeBadRequest(w, "run id and message id are required")
		return
	}
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.mark_run_mailbox_message_read", map[string]any{"run_id": runID})
		return
	}
	message, found, err := s.loadMailboxMessageByID(r.Context(), runID, messageID)
	if err != nil {
		s.writeInternalError(w, "load mailbox message failed")
		return
	}
	if !found {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "mailbox message not found", "api.mark_run_mailbox_message_read", map[string]any{"run_id": runID, "message_id": messageID})
		return
	}
	payload, _, _, _ := parseMailboxPayload(message.PayloadJSON)
	communicationType := mailboxCommunicationType(payload, message)
	if !isUserFacingMailboxMessage(message, payload, communicationType) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "mailbox message not found", "api.mark_run_mailbox_message_read", map[string]any{"run_id": runID, "message_id": messageID})
		return
	}

	mailbox := state.NewMailboxStore(s.db, nil)
	if err := mailbox.MarkRead(r.Context(), state.MarkMailboxMessageReadRequest{RunID: runID, MessageID: messageID, ReadAt: s.now().UTC().Format(time.RFC3339)}); err != nil {
		if strings.Contains(err.Error(), "mailbox message not found") {
			writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "mailbox message not found", "api.mark_run_mailbox_message_read", map[string]any{"run_id": runID, "message_id": messageID})
			return
		}
		s.writeInternalError(w, "mark mailbox message read failed")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) loadMailboxMessageByID(ctx context.Context, runID, messageID string) (state.MailboxMessage, bool, error) {
	var message state.MailboxMessage
	err := s.db.QueryRowContext(
		ctx,
		`SELECT message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at
		 FROM mailbox_messages
		 WHERE run_id = ? AND message_id = ?
		 LIMIT 1`,
		runID,
		messageID,
	).Scan(
		&message.MessageID,
		&message.RunID,
		&message.FromAgentID,
		&message.ToAgentID,
		&message.ThreadID,
		&message.MessageType,
		&message.Priority,
		&message.PayloadJSON,
		&message.CreatedAt,
		&message.ReadAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return state.MailboxMessage{}, false, nil
	}
	if err != nil {
		return state.MailboxMessage{}, false, err
	}
	return message, true, nil
}

func summarizeMailboxMessage(message state.MailboxMessage, agentNames map[string]string) runMailboxMessageSummary {
	payload, format, _, fallbackPreview := parseMailboxPayload(message.PayloadJSON)
	communicationType := mailboxCommunicationType(payload, message)
	preview := mailboxHandoffPreview(payload, communicationType, fallbackPreview)
	nodeID := mailboxNodeID(payload)
	reasonCode := mailboxReasonCode(payload)
	verdictType, verdictStatus := mailboxVerdictSummary(payload, nodeID, reasonCode)
	return runMailboxMessageSummary{
		MessageID:         message.MessageID,
		RunID:             message.RunID,
		NodeID:            nodeID,
		FromAgentID:       message.FromAgentID,
		FromAgentName:     agentDisplayNameFor(agentNames, message.FromAgentID),
		ToAgentID:         message.ToAgentID,
		ToAgentName:       agentDisplayNameFor(agentNames, message.ToAgentID),
		ThreadID:          message.ThreadID,
		MessageType:       string(message.MessageType),
		CommunicationType: communicationType,
		MessageSource:     mailboxview.MessageSource(payload),
		ReasonCode:        nullableMailboxString(reasonCode),
		VerdictType:       nullableMailboxString(verdictType),
		VerdictStatus:     nullableMailboxString(verdictStatus),
		Priority:          string(message.Priority),
		CreatedAt:         message.CreatedAt,
		ReadAt:            nullableString(message.ReadAt),
		Preview:           preview,
		Format:            format,
	}
}

func detailMailboxMessage(message state.MailboxMessage, agentNames map[string]string) runMailboxMessageDetail {
	payload, format, content, _ := parseMailboxPayload(message.PayloadJSON)
	nodeID := mailboxNodeID(payload)
	reasonCode := mailboxReasonCode(payload)
	verdictType, verdictStatus := mailboxVerdictSummary(payload, nodeID, reasonCode)
	return runMailboxMessageDetail{
		MessageID:         message.MessageID,
		RunID:             message.RunID,
		NodeID:            nodeID,
		FromAgentID:       message.FromAgentID,
		FromAgentName:     agentDisplayNameFor(agentNames, message.FromAgentID),
		ToAgentID:         message.ToAgentID,
		ToAgentName:       agentDisplayNameFor(agentNames, message.ToAgentID),
		ThreadID:          message.ThreadID,
		MessageType:       string(message.MessageType),
		CommunicationType: mailboxCommunicationType(payload, message),
		MessageSource:     mailboxview.MessageSource(payload),
		ReasonCode:        nullableMailboxString(reasonCode),
		VerdictType:       nullableMailboxString(verdictType),
		VerdictStatus:     nullableMailboxString(verdictStatus),
		Priority:          string(message.Priority),
		CreatedAt:         message.CreatedAt,
		ReadAt:            nullableString(message.ReadAt),
		Format:            format,
		Content:           content,
		PayloadJSON:       payload,
	}
}

func parseMailboxPayload(payloadJSON string) (any, string, string, string) {
	trimmed := strings.TrimSpace(payloadJSON)
	if trimmed == "" {
		return map[string]any{}, "plain_text", "", ""
	}

	var payload any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return map[string]any{"raw": trimmed}, "plain_text", trimmed, trimPreview(trimmed, 160)
	}

	if object, ok := payload.(map[string]any); ok {
		if markdown, ok := object["body_markdown"].(string); ok && strings.TrimSpace(markdown) != "" {
			return payload, "markdown", markdown, trimPreview(markdown, 160)
		}
		if text, ok := object["body_text"].(string); ok && strings.TrimSpace(text) != "" {
			return payload, "plain_text", text, trimPreview(text, 160)
		}
		for _, key := range []string{"message", "status", "question", "reason_code", "reason"} {
			if candidate, ok := object[key].(string); ok && strings.TrimSpace(candidate) != "" {
				return payload, "plain_text", candidate, trimPreview(candidate, 160)
			}
		}
	}

	pretty, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return payload, "json", trimmed, trimPreview(trimmed, 160)
	}
	content := string(pretty)
	return payload, "json", content, trimPreview(content, 160)
}

func mailboxCommunicationType(payload any, message state.MailboxMessage) string {
	return mailboxview.CommunicationType(payload, string(message.MessageType))
}

func mailboxNodeID(payload any) string {
	return mailboxview.StringField(payload, "node_id")
}

func mailboxReasonCode(payload any) string {
	return mailboxview.ReasonCode(payload)
}

func mailboxVerdictSummary(_ any, nodeID, reasonCode string) (string, string) {
	typeLabel := mailboxVerdictType(nodeID)
	if typeLabel == "" {
		return "", ""
	}

	switch strings.ToLower(strings.TrimSpace(reasonCode)) {
	case "invalid_execution_review_verdict", "invalid_validation_verdict":
		return typeLabel, "invalid"
	case "execution_review_cap_auto_succeeded", "validation_gate_cap_auto_succeeded":
		return typeLabel, "auto_succeeded"
	}

	return typeLabel, ""
}

func mailboxVerdictType(nodeID string) string {
	trimmed := strings.TrimSpace(nodeID)
	if strings.HasPrefix(trimmed, "execution_review") {
		return "review"
	}
	if strings.HasPrefix(trimmed, "validation_gates") {
		return "validation"
	}
	return ""
}

func nullableMailboxString(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func isUserFacingMailboxMessage(message state.MailboxMessage, payload any, communicationType string) bool {
	return mailboxview.IsUserFacingMessage(
		message.FromAgentID,
		message.ToAgentID,
		mailboxview.MessageSource(payload),
		communicationType,
		mailboxReasonCode(payload),
	)
}

func mailboxHandoffPreview(payload any, communicationType string, fallback string) string {
	object, ok := payload.(map[string]any)
	if !ok {
		return fallback
	}

	preferredKey := ""
	switch communicationType {
	case "instruction_dispatched", "approval_request":
		preferredKey = "instruction_full"
	case "response_received", "approval_response":
		preferredKey = "response_full"
	}
	if preferredKey != "" {
		if candidate, ok := object[preferredKey].(string); ok && strings.TrimSpace(candidate) != "" {
			return trimPreview(candidate, 220)
		}
	}
	for _, key := range []string{"instruction_full", "response_full", "body_markdown", "body_text"} {
		if candidate, ok := object[key].(string); ok && strings.TrimSpace(candidate) != "" {
			return trimPreview(candidate, 220)
		}
	}
	return fallback
}

func agentDisplayNameFor(names map[string]string, agentID string) string {
	trimmedID := strings.TrimSpace(agentID)
	if trimmedID == "" {
		return "Unknown Agent"
	}
	if label, ok := names[trimmedID]; ok && strings.TrimSpace(label) != "" {
		return label
	}
	return trimmedID
}

func (s *Server) lookupAgentDisplayNames(ctx context.Context, messages []state.MailboxMessage) (map[string]string, error) {
	names := make(map[string]string)
	if len(messages) == 0 {
		return names, nil
	}

	ids := make([]string, 0)
	seen := make(map[string]struct{})
	for _, message := range messages {
		for _, id := range []string{message.FromAgentID, message.ToAgentID} {
			trimmed := strings.TrimSpace(id)
			if trimmed == "" {
				continue
			}
			if _, ok := seen[trimmed]; ok {
				continue
			}
			seen[trimmed] = struct{}{}
			ids = append(ids, trimmed)
		}
	}
	if len(ids) == 0 {
		return names, nil
	}

	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(
		`SELECT a.agent_id, COALESCE(NULLIF(TRIM(a.display_name), ''), r.display_name, a.agent_id) AS label
		 FROM agents a
		 LEFT JOIN roles r ON r.role_id = a.role_id
		 WHERE a.agent_id IN (%s)`,
		strings.Join(placeholders, ","),
	)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var agentID string
		var label string
		if err := rows.Scan(&agentID, &label); err != nil {
			return nil, err
		}
		names[strings.TrimSpace(agentID)] = strings.TrimSpace(label)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return names, nil
}

func trimPreview(value string, max int) string {
	cleaned := strings.TrimSpace(strings.ReplaceAll(value, "\r\n", "\n"))
	cleaned = strings.ReplaceAll(cleaned, "\r", "\n")
	if len(cleaned) <= max {
		return cleaned
	}
	if max <= 1 {
		return cleaned[:max]
	}
	return cleaned[:max-1] + "…"
}

func nullableString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	copy := value.String
	return &copy
}

func parseMailboxLimit(raw string) (int, error) {
	const (
		defaultMailboxLimit = 50
		maxMailboxLimit     = 200
	)

	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultMailboxLimit, nil
	}
	parsed, err := strconv.Atoi(trimmed)
	if err != nil || parsed <= 0 || parsed > maxMailboxLimit {
		return 0, fmt.Errorf("limit must be an integer between 1 and %d", maxMailboxLimit)
	}
	return parsed, nil
}

func parseOptionalBool(raw string) (bool, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(trimmed)
	if err != nil {
		return false, fmt.Errorf("unread_only must be a boolean")
	}
	return parsed, nil
}
