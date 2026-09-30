package state

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"kingdom/internal/core/rolecomm"
	"kingdom/internal/core/schemas"
	"kingdom/internal/mailboxview"
	"kingdom/internal/sqlitex"
)

type MailboxMessageType string

const (
	MailboxMessageTypeClarification MailboxMessageType = "clarification"
	MailboxMessageTypeEscalation    MailboxMessageType = "escalation"
	MailboxMessageTypeDecision      MailboxMessageType = "decision_request"
	MailboxMessageTypeStatus        MailboxMessageType = "status_update"
)

type MailboxPriority string

const (
	MailboxPriorityLow      MailboxPriority = "low"
	MailboxPriorityNormal   MailboxPriority = "normal"
	MailboxPriorityHigh     MailboxPriority = "high"
	MailboxPriorityCritical MailboxPriority = "critical"
)

type InvalidMailboxMessageError struct {
	Reason string
}

func (e *InvalidMailboxMessageError) Error() string {
	return fmt.Sprintf("invalid mailbox message: %s", e.Reason)
}

type SendMailboxMessageRequest struct {
	MessageID   string
	RunID       string
	FromAgentID string
	ToAgentID   string
	ThreadID    string
	MessageType MailboxMessageType
	Priority    MailboxPriority
	PayloadJSON string
	CreatedAt   string
}

type MarkMailboxMessageReadRequest struct {
	RunID     string
	MessageID string
	ReadAt    string
}

type ListMailboxMessagesRequest struct {
	RunID     string
	ThreadID  string
	ToAgentID string
	Limit     int
}

type MailboxMessage struct {
	MessageID   string
	RunID       string
	FromAgentID string
	ToAgentID   string
	ThreadID    string
	MessageType MailboxMessageType
	Priority    MailboxPriority
	PayloadJSON string
	CreatedAt   string
	ReadAt      sql.NullString
}

type MailboxStore struct {
	db           *sql.DB
	eventSchemas *schemas.Set
}

func NewMailboxStore(db *sql.DB, eventSchemas *schemas.Set) *MailboxStore {
	return &MailboxStore{db: db, eventSchemas: eventSchemas}
}

func (s *MailboxStore) SendMessage(ctx context.Context, req SendMailboxMessageRequest) (MailboxMessage, error) {
	if err := validateMailboxMessageRequest(req); err != nil {
		return MailboxMessage{}, err
	}

	messageID := strings.TrimSpace(req.MessageID)
	if messageID == "" {
		generated, err := newMailboxMessageID()
		if err != nil {
			return MailboxMessage{}, fmt.Errorf("generate mailbox message id: %w", err)
		}
		messageID = generated
	}

	candidate := MailboxMessage{
		MessageID:   messageID,
		RunID:       req.RunID,
		FromAgentID: req.FromAgentID,
		ToAgentID:   req.ToAgentID,
		ThreadID:    req.ThreadID,
		MessageType: req.MessageType,
		Priority:    req.Priority,
		PayloadJSON: req.PayloadJSON,
		CreatedAt:   req.CreatedAt,
	}

	var persisted MailboxMessage
	_, err := sqlitex.Transaction(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		// Identity is generated once, but mutable state must be reloaded after
		// a competing WAL writer invalidates a transaction's read snapshot.
		message := candidate
		existing, found, err := loadMailboxMessageTx(ctx, tx, message.MessageID)
		if err != nil {
			return err
		}
		if found {
			if mailboxMessagesEquivalent(existing, message) {
				persisted = existing
				return nil
			}
			message.ReadAt = existing.ReadAt
			if _, err := tx.ExecContext(ctx, `DELETE FROM mailbox_messages WHERE message_id = ?`, message.MessageID); err != nil {
				return fmt.Errorf("delete superseded mailbox message: %w", err)
			}
		}

		if _, err := tx.ExecContext(ctx,
			`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			message.MessageID,
			message.RunID,
			message.FromAgentID,
			message.ToAgentID,
			message.ThreadID,
			message.MessageType,
			message.Priority,
			message.PayloadJSON,
			message.CreatedAt,
			nullableMailboxSQLString(message.ReadAt),
		); err != nil {
			return fmt.Errorf("insert mailbox message: %w", err)
		}
		if err := upsertRunParticipantTx(ctx, tx, message.RunID, message.FromAgentID, message.CreatedAt); err != nil {
			return err
		}
		if err := upsertRunParticipantTx(ctx, tx, message.RunID, message.ToAgentID, message.CreatedAt); err != nil {
			return err
		}
		messageVersion, err := nextMailboxMessageVersionTx(ctx, tx, message.MessageID)
		if err != nil {
			return err
		}
		if err := s.recordMailboxEventTx(ctx, tx, message, messageVersion); err != nil {
			return err
		}

		persisted = message
		return nil
	}, sqlitex.Options{})
	if err != nil {
		return MailboxMessage{}, err
	}
	return persisted, nil
}

func (s *MailboxStore) MarkRead(ctx context.Context, req MarkMailboxMessageReadRequest) error {
	if strings.TrimSpace(req.RunID) == "" {
		return &InvalidMailboxMessageError{Reason: "run id is required"}
	}
	if strings.TrimSpace(req.MessageID) == "" {
		return &InvalidMailboxMessageError{Reason: "message id is required"}
	}
	if strings.TrimSpace(req.ReadAt) == "" {
		return &InvalidMailboxMessageError{Reason: "read timestamp is required"}
	}

	result, err := s.db.ExecContext(ctx,
		`UPDATE mailbox_messages SET read_at = ? WHERE run_id = ? AND message_id = ?`,
		req.ReadAt,
		req.RunID,
		req.MessageID,
	)
	if err != nil {
		return fmt.Errorf("mark mailbox message read: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mailbox rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("mailbox message not found: %q", req.MessageID)
	}

	return nil
}

func (s *MailboxStore) ListMessages(ctx context.Context, req ListMailboxMessagesRequest) ([]MailboxMessage, error) {
	if strings.TrimSpace(req.RunID) == "" {
		return nil, &InvalidMailboxMessageError{Reason: "run id is required"}
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at
		 FROM mailbox_messages
		 WHERE run_id = ?
		   AND (? = '' OR thread_id = ?)
		   AND (? = '' OR to_agent_id = ?)
		 ORDER BY message_seq DESC
		 LIMIT ?`,
		req.RunID,
		req.ThreadID,
		req.ThreadID,
		req.ToAgentID,
		req.ToAgentID,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list mailbox messages: %w", err)
	}
	defer rows.Close()

	messages := make([]MailboxMessage, 0)
	for rows.Next() {
		var message MailboxMessage
		if err := rows.Scan(
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
		); err != nil {
			return nil, fmt.Errorf("scan mailbox message: %w", err)
		}
		messages = append(messages, message)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate mailbox messages: %w", err)
	}

	return messages, nil
}

func validateMailboxMessageRequest(req SendMailboxMessageRequest) error {
	if strings.TrimSpace(req.RunID) == "" {
		return &InvalidMailboxMessageError{Reason: "run id is required"}
	}
	if strings.TrimSpace(req.FromAgentID) == "" {
		return &InvalidMailboxMessageError{Reason: "from agent id is required"}
	}
	if strings.TrimSpace(req.ToAgentID) == "" {
		return &InvalidMailboxMessageError{Reason: "to agent id is required"}
	}
	if strings.TrimSpace(req.ThreadID) == "" {
		return &InvalidMailboxMessageError{Reason: "thread id is required"}
	}
	if strings.TrimSpace(req.CreatedAt) == "" {
		return &InvalidMailboxMessageError{Reason: "created timestamp is required"}
	}
	if !isValidMailboxMessageType(req.MessageType) {
		return &InvalidMailboxMessageError{Reason: fmt.Sprintf("unsupported message type: %q", req.MessageType)}
	}
	if !isValidMailboxPriority(req.Priority) {
		return &InvalidMailboxMessageError{Reason: fmt.Sprintf("unsupported message priority: %q", req.Priority)}
	}
	if !json.Valid([]byte(req.PayloadJSON)) {
		return &InvalidMailboxMessageError{Reason: "payload_json must be valid JSON"}
	}

	return nil
}

func isValidMailboxMessageType(messageType MailboxMessageType) bool {
	switch messageType {
	case MailboxMessageTypeClarification, MailboxMessageTypeEscalation, MailboxMessageTypeDecision, MailboxMessageTypeStatus:
		return true
	default:
		return false
	}
}

func isValidMailboxPriority(priority MailboxPriority) bool {
	switch priority {
	case MailboxPriorityLow, MailboxPriorityNormal, MailboxPriorityHigh, MailboxPriorityCritical:
		return true
	default:
		return false
	}
}

func (s *MailboxStore) recordMailboxEventTx(ctx context.Context, tx *sql.Tx, message MailboxMessage, messageVersion int) error {
	payload, err := mailboxEventPayload(message, messageVersion)
	if err != nil {
		return err
	}
	payloadRaw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal mailbox event payload: %w", err)
	}

	attemptID := "run:" + strings.TrimSpace(message.RunID) + ":mailbox"
	eventSeq, err := nextEventSeqTx(ctx, tx, attemptID)
	if err != nil {
		return fmt.Errorf("allocate mailbox event sequence: %w", err)
	}
	runEventSeq, err := nextRunEventSeqTx(ctx, tx, message.RunID)
	if err != nil {
		return fmt.Errorf("allocate mailbox run event sequence: %w", err)
	}
	roleID, err := mailboxSenderRoleID(ctx, tx, message.FromAgentID)
	if err != nil {
		return err
	}
	eventID := mailboxEventID(message.MessageID, messageVersion)
	correlationID := "corr_mailbox_" + strings.TrimSpace(message.MessageID)
	causationID := correlationID
	if messageVersion > 1 {
		causationID = fmt.Sprintf("caus_mailbox_%s_v%d", strings.TrimSpace(message.MessageID), messageVersion)
	} else {
		causationID = "caus_mailbox_" + strings.TrimSpace(message.MessageID)
	}
	req := AttemptEventIngestionRequest{
		EventID:         eventID,
		RunID:           strings.TrimSpace(message.RunID),
		NodeRunID:       nullableMailboxString(payload["node_run_id"]),
		AttemptID:       attemptID,
		RunEventSeq:     runEventSeq,
		SchemaVersion:   "v1",
		EventType:       "mailbox.message_recorded",
		Seq:             eventSeq,
		PayloadJSON:     string(payloadRaw),
		ActorID:         strings.TrimSpace(message.FromAgentID),
		RoleID:          roleID,
		AuthoritySource: firstNonEmptyMailboxString(payload["authority_source"], "state.mailbox"),
		ReasonCode:      firstNonEmptyMailboxString(payload["reason_code"], "mailbox_message_recorded"),
		ReasonText:      firstNonEmptyMailboxString(payload["reason_text"], "mailbox message recorded"),
		CorrelationID:   correlationID,
		CausationID:     causationID,
		CreatedAt:       strings.TrimSpace(message.CreatedAt),
	}
	if err := appendAttemptEventTx(ctx, tx, s.eventSchemas, rolecomm.Policy{}, req, attemptEventAppendOptions{mirrorToMailbox: false}); err != nil {
		return fmt.Errorf("append mailbox event %q: %w", req.EventID, err)
	}
	return nil
}

func mailboxEventPayload(message MailboxMessage, messageVersion int) (map[string]any, error) {
	rawPayload, err := mailboxview.DecodePayload(message.PayloadJSON)
	if err != nil {
		return nil, err
	}
	projection := mailboxview.ProjectionFromPayload(rawPayload, message.FromAgentID, message.ToAgentID, string(message.MessageType))
	payload := map[string]any{
		"message_id":          strings.TrimSpace(message.MessageID),
		"run_id":              strings.TrimSpace(message.RunID),
		"from_agent_id":       strings.TrimSpace(message.FromAgentID),
		"to_agent_id":         strings.TrimSpace(message.ToAgentID),
		"thread_id":           strings.TrimSpace(message.ThreadID),
		"message_type":        string(message.MessageType),
		"priority":            string(message.Priority),
		"message_version":     messageVersion,
		"created_at":          strings.TrimSpace(message.CreatedAt),
		"message_source":      projection.MessageSource,
		"communication_type":  projection.CommunicationType,
		"visible_to_operator": projection.VisibleToOperator,
	}
	if projection.NodeID != "" {
		payload["node_id"] = projection.NodeID
	}
	if projection.NodeRunID != "" {
		payload["node_run_id"] = projection.NodeRunID
	}
	if projection.AuthoritySource != "" {
		payload["authority_source"] = projection.AuthoritySource
	}
	if projection.ReasonCode != "" {
		payload["reason_code"] = projection.ReasonCode
	}
	if projection.ReasonText != "" {
		payload["reason_text"] = projection.ReasonText
	}
	return payload, nil
}

func loadMailboxMessageTx(ctx context.Context, tx *sql.Tx, messageID string) (MailboxMessage, bool, error) {
	var message MailboxMessage
	err := tx.QueryRowContext(ctx,
		`SELECT message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at
		 FROM mailbox_messages
		 WHERE message_id = ?`,
		strings.TrimSpace(messageID),
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
		return MailboxMessage{}, false, nil
	}
	if err != nil {
		return MailboxMessage{}, false, fmt.Errorf("load mailbox message %q: %w", messageID, err)
	}
	return message, true, nil
}

func mailboxMessagesEquivalent(existing, incoming MailboxMessage) bool {
	return strings.TrimSpace(existing.RunID) == strings.TrimSpace(incoming.RunID) &&
		strings.TrimSpace(existing.FromAgentID) == strings.TrimSpace(incoming.FromAgentID) &&
		strings.TrimSpace(existing.ToAgentID) == strings.TrimSpace(incoming.ToAgentID) &&
		strings.TrimSpace(existing.ThreadID) == strings.TrimSpace(incoming.ThreadID) &&
		existing.MessageType == incoming.MessageType &&
		existing.Priority == incoming.Priority &&
		strings.TrimSpace(existing.PayloadJSON) == strings.TrimSpace(incoming.PayloadJSON)
}

func nextMailboxMessageVersionTx(ctx context.Context, tx *sql.Tx, messageID string) (int, error) {
	var existingCount int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(1)
		 FROM events
		 WHERE event_type = 'mailbox.message_recorded'
		   AND json_extract(payload_json, '$.message_id') = ?`,
		strings.TrimSpace(messageID),
	).Scan(&existingCount); err != nil {
		return 0, fmt.Errorf("count mailbox event revisions for %q: %w", messageID, err)
	}
	return existingCount + 1, nil
}

func mailboxEventID(messageID string, messageVersion int) string {
	trimmedMessageID := strings.TrimSpace(messageID)
	if messageVersion <= 1 {
		return "evt_mailbox_" + trimmedMessageID
	}
	return fmt.Sprintf("evt_mailbox_%s_v%d", trimmedMessageID, messageVersion)
}

func nullableMailboxSQLString(value sql.NullString) any {
	if !value.Valid {
		return nil
	}
	return strings.TrimSpace(value.String)
}

func mailboxSenderRoleID(ctx context.Context, tx *sql.Tx, agentID string) (string, error) {
	var roleID string
	if err := tx.QueryRowContext(ctx,
		`SELECT role_id FROM agents WHERE agent_id = ?`,
		strings.TrimSpace(agentID),
	).Scan(&roleID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("mailbox sender agent not found: %q", agentID)
		}
		return "", fmt.Errorf("query mailbox sender role: %w", err)
	}
	return roleID, nil
}

func nullableMailboxString(value any) sql.NullString {
	trimmed := firstNonEmptyMailboxString(value)
	if trimmed == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: trimmed, Valid: true}
}

func firstNonEmptyMailboxString(values ...any) string {
	for _, value := range values {
		if text, ok := value.(string); ok {
			if trimmed := strings.TrimSpace(text); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func newMailboxMessageID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "msg_" + hex.EncodeToString(b), nil
}
