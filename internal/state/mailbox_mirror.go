package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"kingdom/internal/core/rolecomm"
)

var mailboxMirrorNonAlphaNum = regexp.MustCompile(`[^a-z0-9]+`)

type EventMailboxMirrorRequest struct {
	EventID     string
	RunID       string
	NodeRunID   sql.NullString
	FromState   string
	ToState     string
	AttemptID   string
	EventType   string
	PayloadJSON string
	RoleID      string
	ReasonCode  string
	ReasonText  string
	CreatedAt   string
}

func MirrorEventToMailbox(ctx context.Context, tx *sql.Tx, policy rolecomm.Policy, req EventMailboxMirrorRequest) error {
	if tx == nil {
		return fmt.Errorf("mailbox mirror transaction is required")
	}
	runID := strings.TrimSpace(req.RunID)
	eventID := strings.TrimSpace(req.EventID)
	roleID := strings.TrimSpace(req.RoleID)
	createdAt := strings.TrimSpace(req.CreatedAt)
	if runID == "" || eventID == "" || roleID == "" || createdAt == "" {
		return fmt.Errorf("mailbox mirror requires run_id, event_id, role_id, and created_at")
	}

	fromRole := roleID
	toRole := resolveMirrorRecipientRole(ctx, tx, fromRole)

	fromAgentID := mailboxAgentIDForRole(fromRole)
	toAgentID := mailboxAgentIDForRole(toRole)

	if err := ensureMirrorAgent(ctx, tx, fromAgentID, fromRole, createdAt); err != nil {
		return err
	}
	if err := ensureMirrorAgent(ctx, tx, toAgentID, toRole, createdAt); err != nil {
		return err
	}

	messageType := mailboxMessageTypeForEvent(req.EventType, req.ReasonCode)
	priority := mailboxPriorityForEvent(req.EventType, req.ReasonCode)
	threadID := mailboxThreadIDForEvent(runID, req.AttemptID)
	body := renderMailboxRoleplayBody(policy, fromRole, toRole, req)

	payload := map[string]any{
		"body_markdown":  body,
		"message_source": "mirrored_event",
		"event_id":       eventID,
		"event_type":     strings.TrimSpace(req.EventType),
		"run_id":         runID,
		"node_run_id":    strings.TrimSpace(req.NodeRunID.String),
		"attempt_id":     strings.TrimSpace(req.AttemptID),
		"reason_code":    strings.TrimSpace(req.ReasonCode),
		"reason_text":    strings.TrimSpace(req.ReasonText),
		"sender_role":    fromRole,
		"recipient_role": toRole,
	}
	if trimmedPayload := strings.TrimSpace(req.PayloadJSON); trimmedPayload != "" {
		payload["event_payload_raw"] = trimmedPayload
	}
	if strings.TrimSpace(req.FromState) != "" {
		payload["from_state"] = strings.TrimSpace(req.FromState)
	}
	if strings.TrimSpace(req.ToState) != "" {
		payload["to_state"] = strings.TrimSpace(req.ToState)
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal mirrored mailbox payload: %w", err)
	}

	messageID := "msg_evt_" + sanitizeMailboxToken(eventID)
	_, err = tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		messageID,
		runID,
		fromAgentID,
		toAgentID,
		threadID,
		messageType,
		priority,
		string(payloadBytes),
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("insert mirrored mailbox message: %w", err)
	}

	return nil
}

func resolveMirrorRecipientRole(ctx context.Context, tx *sql.Tx, fromRole string) string {
	candidate := mirrorRecipientRoleForSender(fromRole)
	if mirrorRoleExists(ctx, tx, candidate) {
		return candidate
	}
	if mirrorRoleExists(ctx, tx, fromRole) {
		return fromRole
	}
	if mirrorRoleExists(ctx, tx, "plan-author") {
		return "plan-author"
	}
	if mirrorRoleExists(ctx, tx, "coordinator") {
		return "coordinator"
	}
	if mirrorRoleExists(ctx, tx, "governance") {
		return "governance"
	}
	return fromRole
}

func mirrorRecipientRoleForSender(roleID string) string {
	trimmed := strings.TrimSpace(strings.ToLower(roleID))
	switch trimmed {
	case "governance":
		return "coordinator"
	case "coordinator":
		return "plan-author"
	case "plan-author":
		return "coordinator"
	default:
		if strings.HasPrefix(trimmed, "intelligence") || strings.HasPrefix(trimmed, "builder") {
			return "plan-author"
		}
		return "coordinator"
	}
}

func mirrorRoleExists(ctx context.Context, tx *sql.Tx, roleID string) bool {
	var marker int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM roles WHERE role_id = ? LIMIT 1`, roleID).Scan(&marker)
	return err == nil
}

func ensureMirrorAgent(ctx context.Context, tx *sql.Tx, agentID, roleID, occurredAt string) error {
	displayName := roleDisplayNameByID(roleID)
	_, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at)
		 VALUES (?, ?, ?, 'idle', NULL, 0, ?, ?)`,
		agentID,
		roleID,
		displayName,
		occurredAt,
		occurredAt,
	)
	if err != nil {
		return fmt.Errorf("ensure mirror agent %q for role %q: %w", agentID, roleID, err)
	}
	return nil
}

func roleDisplayNameByID(roleID string) string {
	trimmed := strings.TrimSpace(roleID)
	for _, role := range canonicalRoles {
		if role.RoleID == trimmed {
			return role.DisplayName
		}
	}
	return roleTitleFromID(trimmed)
}

func mailboxMessageTypeForEvent(eventType, reasonCode string) MailboxMessageType {
	event := strings.ToLower(strings.TrimSpace(eventType))
	reason := strings.ToLower(strings.TrimSpace(reasonCode))
	if strings.Contains(event, "escalation") || reason == "policy_violation" {
		return MailboxMessageTypeEscalation
	}
	if strings.Contains(event, "approval") || strings.Contains(event, "decision") {
		return MailboxMessageTypeDecision
	}
	if strings.Contains(event, "clarification") || strings.Contains(event, "question") {
		return MailboxMessageTypeClarification
	}
	return MailboxMessageTypeStatus
}

func mailboxPriorityForEvent(eventType, reasonCode string) MailboxPriority {
	event := strings.ToLower(strings.TrimSpace(eventType))
	reason := strings.ToLower(strings.TrimSpace(reasonCode))
	if strings.Contains(event, "escalation") || strings.Contains(event, "failed") || reason == "policy_violation" {
		return MailboxPriorityHigh
	}
	if strings.Contains(event, "timeout") {
		return MailboxPriorityHigh
	}
	return MailboxPriorityNormal
}

func mailboxThreadIDForEvent(runID, attemptID string) string {
	trimmedAttempt := strings.TrimSpace(attemptID)
	if trimmedAttempt == "" {
		return "thread_" + sanitizeMailboxToken(runID) + "_governance"
	}
	return "thread_" + sanitizeMailboxToken(runID) + "_" + sanitizeMailboxToken(trimmedAttempt)
}

func renderMailboxRoleplayBody(policy rolecomm.Policy, fromRole, toRole string, req EventMailboxMirrorRequest) string {
	templateConfig := policy.MailboxTemplateForRole(fromRole)
	toTitle := roleTitleFromID(toRole)
	fromTitle := roleTitleFromID(fromRole)
	data := map[string]string{
		"RunID":          strings.TrimSpace(req.RunID),
		"EventID":        strings.TrimSpace(req.EventID),
		"EventType":      strings.TrimSpace(req.EventType),
		"NodeRunID":      strings.TrimSpace(req.NodeRunID.String),
		"AttemptID":      strings.TrimSpace(req.AttemptID),
		"FromState":      strings.TrimSpace(req.FromState),
		"ToState":        strings.TrimSpace(req.ToState),
		"ReasonCode":     strings.TrimSpace(req.ReasonCode),
		"ReasonText":     firstNonEmptyMirror(mirrorOperationalSummary(req), strings.TrimSpace(req.ReasonText), "The court record has been updated accordingly."),
		"SenderRole":     strings.TrimSpace(fromRole),
		"SenderTitle":    fromTitle,
		"RecipientRole":  strings.TrimSpace(toRole),
		"RecipientTitle": toTitle,
	}
	opening := rolecomm.RenderTemplate(templateConfig.SalutationTemplate, data, fmt.Sprintf("To the %s,", toTitle))
	body := rolecomm.RenderTemplate(templateConfig.BodyTemplate, data, fmt.Sprintf("I report `%s` for run `%s`.\n%s", data["EventType"], data["RunID"], data["ReasonText"]))
	signoff := rolecomm.RenderTemplate(templateConfig.SignoffTemplate, data, "In service,")
	signature := rolecomm.RenderTemplate(templateConfig.SignatureTemplate, data, fromTitle)
	return strings.Join([]string{
		opening,
		"",
		body,
		"",
		signoff,
		signature,
	}, "\n")
}

func mirrorOperationalSummary(req EventMailboxMirrorRequest) string {
	fromState := strings.TrimSpace(req.FromState)
	toState := strings.TrimSpace(req.ToState)
	if fromState != "" && toState != "" {
		return fmt.Sprintf("state transition: %s -> %s", fromState, toState)
	}

	payloadRaw := strings.TrimSpace(req.PayloadJSON)
	if payloadRaw == "" {
		return ""
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadRaw), &payload); err != nil {
		return ""
	}

	if planState, ok := payload["plan_state"].(string); ok && strings.TrimSpace(planState) != "" {
		return fmt.Sprintf("plan state is now %s", strings.TrimSpace(planState))
	}
	if status, ok := payload["status"].(string); ok && strings.TrimSpace(status) != "" {
		return fmt.Sprintf("run status is now %s", strings.TrimSpace(status))
	}
	if nodeID, ok := payload["node_id"].(string); ok && strings.TrimSpace(nodeID) != "" {
		if attemptNumber, ok := payload["attempt"].(float64); ok {
			return fmt.Sprintf("node %s attempt %d updated", strings.TrimSpace(nodeID), int(attemptNumber))
		}
		return fmt.Sprintf("node %s updated", strings.TrimSpace(nodeID))
	}

	return ""
}

func roleTitleFromID(roleID string) string {
	trimmed := strings.TrimSpace(roleID)
	if trimmed == "" {
		return "Court"
	}
	parts := strings.Split(trimmed, "-")
	for idx, part := range parts {
		if part == "" {
			continue
		}
		switch strings.ToLower(part) {
		case "qa", "api", "ui", "id":
			parts[idx] = strings.ToUpper(part)
			continue
		}
		parts[idx] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, " ")
}

func mailboxAgentIDForRole(roleID string) string {
	return "agent_" + sanitizeMailboxToken(roleID)
}

func sanitizeMailboxToken(value string) string {
	trimmed := strings.ToLower(strings.TrimSpace(value))
	clean := mailboxMirrorNonAlphaNum.ReplaceAllString(trimmed, "_")
	clean = strings.Trim(clean, "_")
	if clean == "" {
		return "unknown"
	}
	return clean
}

func firstNonEmptyMirror(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
