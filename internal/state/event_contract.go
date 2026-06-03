package state

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"kingdom/internal/core/schemas"
)

type EventContractValidationError struct {
	Reason string
	err    error
}

func (e *EventContractValidationError) Error() string {
	message := "event contract validation failed"
	if strings.TrimSpace(e.Reason) != "" {
		message = fmt.Sprintf("%s: %s", message, e.Reason)
	}
	if e.err != nil {
		return fmt.Sprintf("%s: %v", message, e.err)
	}
	return message
}

func (e *EventContractValidationError) Unwrap() error {
	return e.err
}

type eventContractEnvelope struct {
	EventID         string          `json:"event_id"`
	SchemaVersion   string          `json:"schema_version"`
	RunID           string          `json:"run_id"`
	RunEventSeq     int             `json:"run_event_seq"`
	NodeRunID       *string         `json:"node_run_id,omitempty"`
	AttemptID       string          `json:"attempt_id"`
	Seq             int             `json:"seq"`
	CreatedAt       string          `json:"created_at"`
	EventType       string          `json:"event_type"`
	ActorID         string          `json:"actor_id"`
	RoleID          string          `json:"role_id"`
	AuthoritySource string          `json:"authority_source"`
	ReasonCode      string          `json:"reason_code"`
	ReasonText      string          `json:"reason_text"`
	CorrelationID   string          `json:"correlation_id"`
	CausationID     string          `json:"causation_id"`
	PayloadJSON     json.RawMessage `json:"payload_json"`
}

func validateTransitionEventContract(schemaSet *schemas.Set, req TransitionRequest) error {
	envelope, err := buildEventContractEnvelope(
		req.EventID,
		req.SchemaVersion,
		req.RunID,
		req.RunEventSeq,
		req.NodeRunID,
		req.AttemptID,
		req.EventSeq,
		req.OccurredAt,
		req.EventType,
		req.ActorID,
		req.RoleID,
		req.AuthoritySource,
		req.ReasonCode,
		req.ReasonText,
		req.CorrelationID,
		req.CausationID,
		req.PayloadJSON,
	)
	if err != nil {
		return err
	}
	return validateEventContractEnvelope(schemaSet, envelope)
}

func validateAttemptEventContract(schemaSet *schemas.Set, req AttemptEventIngestionRequest) error {
	envelope, err := buildEventContractEnvelope(
		req.EventID,
		req.SchemaVersion,
		req.RunID,
		req.RunEventSeq,
		req.NodeRunID,
		req.AttemptID,
		req.Seq,
		req.CreatedAt,
		req.EventType,
		req.ActorID,
		req.RoleID,
		req.AuthoritySource,
		req.ReasonCode,
		req.ReasonText,
		req.CorrelationID,
		req.CausationID,
		req.PayloadJSON,
	)
	if err != nil {
		return err
	}
	return validateEventContractEnvelope(schemaSet, envelope)
}

func buildEventContractEnvelope(eventID, schemaVersion, runID string, runEventSeq int, nodeRunID sql.NullString, attemptID string, seq int, createdAt, eventType, actorID, roleID, authoritySource, reasonCode, reasonText, correlationID, causationID, payloadJSON string) (eventContractEnvelope, error) {
	trimmedPayload := strings.TrimSpace(payloadJSON)
	envelope := eventContractEnvelope{
		EventID:         strings.TrimSpace(eventID),
		SchemaVersion:   strings.TrimSpace(schemaVersion),
		RunID:           strings.TrimSpace(runID),
		RunEventSeq:     runEventSeq,
		AttemptID:       strings.TrimSpace(attemptID),
		Seq:             seq,
		CreatedAt:       strings.TrimSpace(createdAt),
		EventType:       strings.TrimSpace(eventType),
		ActorID:         strings.TrimSpace(actorID),
		RoleID:          strings.TrimSpace(roleID),
		AuthoritySource: strings.TrimSpace(authoritySource),
		ReasonCode:      strings.TrimSpace(reasonCode),
		ReasonText:      strings.TrimSpace(reasonText),
		CorrelationID:   strings.TrimSpace(correlationID),
		CausationID:     strings.TrimSpace(causationID),
		PayloadJSON:     json.RawMessage(trimmedPayload),
	}
	if nodeRunID.Valid {
		trimmed := strings.TrimSpace(nodeRunID.String)
		envelope.NodeRunID = &trimmed
	}
	if !json.Valid(envelope.PayloadJSON) {
		return eventContractEnvelope{}, &EventContractValidationError{
			Reason: "payload_json must be valid JSON",
			err:    fmt.Errorf("invalid payload JSON"),
		}
	}
	return envelope, nil
}

func validateEventContractEnvelope(schemaSet *schemas.Set, envelope eventContractEnvelope) error {
	if schemaSet == nil {
		return nil
	}

	raw, err := json.Marshal(envelope)
	if err != nil {
		return &EventContractValidationError{Reason: "marshal canonical event envelope", err: err}
	}
	if err := schemaSet.ValidateJSONBytes("event_v1", raw); err != nil {
		return &EventContractValidationError{Reason: "canonical event envelope rejected by event_v1", err: err}
	}
	return nil
}
