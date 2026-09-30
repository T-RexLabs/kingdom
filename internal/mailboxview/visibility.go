package mailboxview

import (
	"encoding/json"
	"fmt"
	"strings"
)

var userFacingCommunicationTypes = map[string]struct{}{
	"instruction_dispatched": {},
	"response_received":      {},
	"approval_request":       {},
	"approval_response":      {},
}

var suppressedReasonCodes = map[string]struct{}{
	"advisor_scope_drift":          {},
	"governance_instruction_drift": {},
	"planning_scope_drift":          {},
}

type Projection struct {
	AuthoritySource   string
	CommunicationType string
	MessageSource     string
	NodeID            string
	NodeRunID         string
	ReasonCode        string
	ReasonText        string
	VisibleToOperator bool
}

func DecodePayload(payloadJSON string) (map[string]any, error) {
	trimmed := strings.TrimSpace(payloadJSON)
	if trimmed == "" {
		return map[string]any{}, nil
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return nil, fmt.Errorf("decode mailbox payload: %w", err)
	}
	return payload, nil
}

func ProjectionFromPayload(payload any, fromAgentID, toAgentID, fallbackMessageType string) Projection {
	messageSource := MessageSource(payload)
	communicationType := CommunicationType(payload, fallbackMessageType)
	reasonCode := ReasonCode(payload)
	return Projection{
		AuthoritySource:   StringField(payload, "authority_source"),
		CommunicationType: communicationType,
		MessageSource:     messageSource,
		NodeID:            StringField(payload, "node_id"),
		NodeRunID:         StringField(payload, "node_run_id"),
		ReasonCode:        reasonCode,
		ReasonText:        StringField(payload, "reason_text"),
		VisibleToOperator: IsUserFacingMessage(fromAgentID, toAgentID, messageSource, communicationType, reasonCode),
	}
}

func MessageSource(payload any) string {
	trimmed := strings.ToLower(StringField(payload, "message_source"))
	if trimmed == "" {
		return "system"
	}
	return trimmed
}

func CommunicationType(payload any, fallbackMessageType string) string {
	trimmed := strings.ToLower(StringField(payload, "communication_type"))
	if trimmed == "" {
		return strings.ToLower(strings.TrimSpace(fallbackMessageType))
	}
	return trimmed
}

func ReasonCode(payload any) string {
	return strings.ToLower(StringField(payload, "reason_code"))
}

func StringField(payload any, key string) string {
	object, ok := payload.(map[string]any)
	if !ok {
		return ""
	}
	value, ok := object[key].(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func IsUserFacingMessage(fromAgentID, toAgentID, messageSource, communicationType, reasonCode string) bool {
	if strings.TrimSpace(fromAgentID) == strings.TrimSpace(toAgentID) {
		return false
	}
	if strings.ToLower(strings.TrimSpace(messageSource)) != "communication" {
		return false
	}
	if _, ok := userFacingCommunicationTypes[strings.ToLower(strings.TrimSpace(communicationType))]; !ok {
		return false
	}
	if _, ok := suppressedReasonCodes[strings.ToLower(strings.TrimSpace(reasonCode))]; ok {
		return false
	}
	return true
}

func UserFacingMailboxPredicate(alias string) string {
	prefix := ""
	if trimmed := strings.TrimSpace(alias); trimmed != "" {
		prefix = trimmed + "."
	}
	payloadColumn := prefix + "payload_json"
	return fmt.Sprintf(`COALESCE(%sfrom_agent_id, '') != COALESCE(%sto_agent_id, '')
	  AND lower(COALESCE(json_extract(%s, '$.message_source'), '')) = 'communication'
	  AND lower(COALESCE(json_extract(%s, '$.communication_type'), '')) IN ('instruction_dispatched', 'response_received', 'approval_request', 'approval_response')
	  AND lower(COALESCE(json_extract(%s, '$.reason_code'), '')) NOT IN ('advisor_scope_drift', 'governance_instruction_drift', 'planning_scope_drift')`, prefix, prefix, payloadColumn, payloadColumn, payloadColumn)
}
