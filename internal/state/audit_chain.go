package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"kingdom/internal/timestamporder"
)

const (
	AuditGenesisPrevHash                = "0000000000000000000000000000000000000000000000000000000000000000"
	ExternalIntegrationAuditHashVersion = "v1"
)

type AuditIntegrityFailure struct {
	Stream   string
	RunID    string
	RecordID string
	Reason   string
}

type VerifyAuditIntegrityResult struct {
	TransitionsVerified               int
	EventsVerified                    int
	ExternalIntegrationEventsVerified int
	Failures                          []AuditIntegrityFailure
}

type AuditIntegrityError struct {
	Failures []AuditIntegrityFailure
}

func (e *AuditIntegrityError) Error() string {
	if len(e.Failures) == 0 {
		return "audit integrity verification failed"
	}
	first := e.Failures[0]
	return fmt.Sprintf("audit integrity verification failed (%d failures), first=%s run=%q record=%q: %s", len(e.Failures), first.Stream, first.RunID, first.RecordID, first.Reason)
}

type AuditIntegrityVerifier struct {
	db *sql.DB
}

func NewAuditIntegrityVerifier(db *sql.DB) *AuditIntegrityVerifier {
	return &AuditIntegrityVerifier{db: db}
}

func (v *AuditIntegrityVerifier) Verify(ctx context.Context) (VerifyAuditIntegrityResult, error) {
	transitionsVerified, transitionFailures, err := verifyTransitionChain(ctx, v.db)
	if err != nil {
		return VerifyAuditIntegrityResult{}, err
	}

	eventsVerified, eventFailures, err := verifyEventChain(ctx, v.db)
	if err != nil {
		return VerifyAuditIntegrityResult{}, err
	}

	externalVerified, externalFailures, err := verifyExternalIntegrationAuditChain(ctx, v.db)
	if err != nil {
		return VerifyAuditIntegrityResult{}, err
	}

	failures := make([]AuditIntegrityFailure, 0, len(transitionFailures)+len(eventFailures)+len(externalFailures))
	failures = append(failures, transitionFailures...)
	failures = append(failures, eventFailures...)
	failures = append(failures, externalFailures...)

	return VerifyAuditIntegrityResult{
		TransitionsVerified:               transitionsVerified,
		EventsVerified:                    eventsVerified,
		ExternalIntegrationEventsVerified: externalVerified,
		Failures:                          failures,
	}, nil
}

type externalIntegrationAuditRecord struct {
	EventID, ConnectionID, SourceID, EventType, ActorID string
	SafePayloadJSON, PreviousHash, EventHash, CreatedAt string
	Sequence                                            int
	HashVersion                                         string
}

func externalIntegrationEventHash(rec externalIntegrationAuditRecord) string {
	sum := sha256.Sum256([]byte(rec.PreviousHash + "\n" + rec.EventID + "\n" + rec.ConnectionID + "\n" + rec.SourceID + "\n" + rec.EventType + "\n" + rec.ActorID + "\n" + rec.SafePayloadJSON + "\n" + rec.CreatedAt))
	return hex.EncodeToString(sum[:])
}

func verifyExternalIntegrationAuditChain(ctx context.Context, db *sql.DB) (int, []AuditIntegrityFailure, error) {
	rows, err := db.QueryContext(ctx, `SELECT audit_event_id,event_sequence,hash_version,COALESCE(connection_id,''),COALESCE(source_id,''),event_type,actor_id,safe_payload_json,previous_hash,event_hash,created_at FROM external_integration_audit_events ORDER BY event_sequence`)
	if err != nil {
		return 0, nil, fmt.Errorf("query external integration audit events: %w", err)
	}
	expectedPrevious := AuditGenesisPrevHash
	expectedSequence := 1
	verified := 0
	var failures []AuditIntegrityFailure
	for rows.Next() {
		var rec externalIntegrationAuditRecord
		if err := rows.Scan(&rec.EventID, &rec.Sequence, &rec.HashVersion, &rec.ConnectionID, &rec.SourceID, &rec.EventType, &rec.ActorID, &rec.SafePayloadJSON, &rec.PreviousHash, &rec.EventHash, &rec.CreatedAt); err != nil {
			_ = rows.Close()
			return 0, nil, fmt.Errorf("scan external integration audit event: %w", err)
		}
		if rec.Sequence != expectedSequence {
			failures = append(failures, AuditIntegrityFailure{Stream: "external_integration_audit_events", RunID: rec.ConnectionID, RecordID: rec.EventID, Reason: fmt.Sprintf("event_sequence gap: expected %d got %d", expectedSequence, rec.Sequence)})
		}
		if rec.HashVersion != ExternalIntegrationAuditHashVersion {
			failures = append(failures, AuditIntegrityFailure{Stream: "external_integration_audit_events", RunID: rec.ConnectionID, RecordID: rec.EventID, Reason: fmt.Sprintf("unsupported hash_version: expected %q got %q", ExternalIntegrationAuditHashVersion, rec.HashVersion)})
		}
		if rec.PreviousHash != expectedPrevious {
			failures = append(failures, AuditIntegrityFailure{Stream: "external_integration_audit_events", RunID: rec.ConnectionID, RecordID: rec.EventID, Reason: fmt.Sprintf("previous_hash mismatch: expected %q got %q", expectedPrevious, rec.PreviousHash)})
		}
		expectedHash := externalIntegrationEventHash(rec)
		if rec.EventHash != expectedHash {
			failures = append(failures, AuditIntegrityFailure{Stream: "external_integration_audit_events", RunID: rec.ConnectionID, RecordID: rec.EventID, Reason: fmt.Sprintf("event_hash mismatch: expected %q got %q", expectedHash, rec.EventHash)})
		}
		expectedPrevious = rec.EventHash
		expectedSequence = rec.Sequence + 1
		verified++
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, nil, fmt.Errorf("iterate external integration audit events: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, nil, fmt.Errorf("close external integration audit events: %w", err)
	}

	var headSequence int
	var headHash string
	if err := db.QueryRowContext(ctx, `SELECT last_sequence,last_hash FROM external_integration_audit_head WHERE head_id=1`).Scan(&headSequence, &headHash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			failures = append(failures, AuditIntegrityFailure{Stream: "external_integration_audit_head", RecordID: "1", Reason: "durable head is missing"})
			return verified, failures, nil
		}
		return 0, nil, fmt.Errorf("query external integration audit head: %w", err)
	}
	expectedHeadSequence := verified
	expectedHeadHash := expectedPrevious
	if headSequence != expectedHeadSequence {
		failures = append(failures, AuditIntegrityFailure{Stream: "external_integration_audit_head", RecordID: "1", Reason: fmt.Sprintf("last_sequence mismatch: expected %d got %d", expectedHeadSequence, headSequence)})
	}
	if headHash != expectedHeadHash {
		failures = append(failures, AuditIntegrityFailure{Stream: "external_integration_audit_head", RecordID: "1", Reason: fmt.Sprintf("last_hash mismatch: expected %q got %q", expectedHeadHash, headHash)})
	}
	return verified, failures, nil
}

func (v *AuditIntegrityVerifier) VerifyOrError(ctx context.Context) error {
	result, err := v.Verify(ctx)
	if err != nil {
		return err
	}
	if len(result.Failures) > 0 {
		return &AuditIntegrityError{Failures: result.Failures}
	}
	return nil
}

type transitionAuditRecord struct {
	TransitionID    string
	RunID           string
	NodeRunID       sql.NullString
	EntityType      string
	FromState       string
	ToState         string
	ActorID         string
	RoleID          string
	AuthoritySource string
	ReasonCode      string
	ReasonText      string
	CorrelationID   string
	CausationID     string
	CreatedAt       string
	PrevHash        string
	EntryHash       string
}

func assignTransitionHashes(ctx context.Context, tx *sql.Tx, req *TransitionRequest) error {
	prevHash, err := loadPreviousTransitionHash(ctx, tx, req.RunID, req.OccurredAt, req.TransitionID)
	if err != nil {
		return err
	}

	canonical, err := canonicalTransitionJSON(transitionAuditRecord{
		TransitionID:    req.TransitionID,
		RunID:           req.RunID,
		NodeRunID:       req.NodeRunID,
		EntityType:      string(req.Entity),
		FromState:       req.FromState,
		ToState:         req.ToState,
		ActorID:         req.ActorID,
		RoleID:          req.RoleID,
		AuthoritySource: req.AuthoritySource,
		ReasonCode:      req.ReasonCode,
		ReasonText:      req.ReasonText,
		CorrelationID:   req.CorrelationID,
		CausationID:     req.CausationID,
		CreatedAt:       req.OccurredAt,
	})
	if err != nil {
		return err
	}

	req.TransitionPrevHash = prevHash
	req.TransitionEntryHash = entryHashFromCanonical(prevHash, canonical)
	return nil
}

type eventAuditRecord struct {
	EventID          string
	RunID            string
	RunEventSeq      int
	AttemptID        string
	EventType        string
	Seq              int
	PayloadJSON      string
	ActorID          string
	RoleID           string
	AuthoritySource  string
	ReasonCode       string
	ReasonText       string
	CorrelationID    string
	CausationID      string
	CreatedAt        string
	PrevHash         string
	EntryHash        string
	SchemaVersion    string
	NodeRunID        sql.NullString
	IncludePrevEntry bool
}

func assignTransitionEventHashes(ctx context.Context, tx *sql.Tx, req *TransitionRequest) error {
	prevHash, err := loadPreviousEventHash(ctx, tx, req.RunID, req.RunEventSeq)
	if err != nil {
		return err
	}

	canonical, err := canonicalEventJSON(eventAuditRecord{
		EventID:         req.EventID,
		RunID:           req.RunID,
		RunEventSeq:     req.RunEventSeq,
		NodeRunID:       req.NodeRunID,
		AttemptID:       req.AttemptID,
		SchemaVersion:   req.SchemaVersion,
		EventType:       req.EventType,
		Seq:             req.EventSeq,
		PayloadJSON:     req.PayloadJSON,
		ActorID:         req.ActorID,
		RoleID:          req.RoleID,
		AuthoritySource: req.AuthoritySource,
		ReasonCode:      req.ReasonCode,
		ReasonText:      req.ReasonText,
		CorrelationID:   req.CorrelationID,
		CausationID:     req.CausationID,
		CreatedAt:       req.OccurredAt,
	})
	if err != nil {
		return err
	}

	req.EventPrevHash = prevHash
	req.EventEntryHash = entryHashFromCanonical(prevHash, canonical)
	return nil
}

func assignIngestionEventHashes(ctx context.Context, tx *sql.Tx, req *AttemptEventIngestionRequest) error {
	prevHash, err := loadPreviousEventHash(ctx, tx, req.RunID, req.RunEventSeq)
	if err != nil {
		return err
	}

	canonical, err := canonicalEventJSON(eventAuditRecord{
		EventID:         req.EventID,
		RunID:           req.RunID,
		RunEventSeq:     req.RunEventSeq,
		NodeRunID:       req.NodeRunID,
		AttemptID:       req.AttemptID,
		SchemaVersion:   req.SchemaVersion,
		EventType:       req.EventType,
		Seq:             req.Seq,
		PayloadJSON:     req.PayloadJSON,
		ActorID:         req.ActorID,
		RoleID:          req.RoleID,
		AuthoritySource: req.AuthoritySource,
		ReasonCode:      req.ReasonCode,
		ReasonText:      req.ReasonText,
		CorrelationID:   req.CorrelationID,
		CausationID:     req.CausationID,
		CreatedAt:       req.CreatedAt,
	})
	if err != nil {
		return err
	}

	req.PrevHash = prevHash
	req.EntryHash = entryHashFromCanonical(prevHash, canonical)
	return nil
}

func verifyTransitionChain(ctx context.Context, db *sql.DB) (int, []AuditIntegrityFailure, error) {
	query := `SELECT transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, created_at, prev_hash, entry_hash
		 FROM transitions
		 ORDER BY run_id ASC, ` + timestamporder.OrderBySQL("created_at", false) + `, transition_id ASC`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return 0, nil, fmt.Errorf("query transitions for audit verification: %w", err)
	}
	defer rows.Close()

	var (
		currentRunID string
		expectedPrev = AuditGenesisPrevHash
		verified     int
		failures     []AuditIntegrityFailure
	)

	for rows.Next() {
		var rec transitionAuditRecord
		if err := rows.Scan(
			&rec.TransitionID,
			&rec.RunID,
			&rec.NodeRunID,
			&rec.EntityType,
			&rec.FromState,
			&rec.ToState,
			&rec.ActorID,
			&rec.RoleID,
			&rec.AuthoritySource,
			&rec.ReasonCode,
			&rec.ReasonText,
			&rec.CorrelationID,
			&rec.CausationID,
			&rec.CreatedAt,
			&rec.PrevHash,
			&rec.EntryHash,
		); err != nil {
			return 0, nil, fmt.Errorf("scan transition audit record: %w", err)
		}

		if rec.RunID != currentRunID {
			currentRunID = rec.RunID
			expectedPrev = AuditGenesisPrevHash
		}

		if rec.PrevHash != expectedPrev {
			failures = append(failures, AuditIntegrityFailure{
				Stream:   "transitions",
				RunID:    rec.RunID,
				RecordID: rec.TransitionID,
				Reason:   fmt.Sprintf("prev_hash mismatch: expected %q got %q", expectedPrev, rec.PrevHash),
			})
		}

		canonical, canonicalErr := canonicalTransitionJSON(rec)
		if canonicalErr != nil {
			failures = append(failures, AuditIntegrityFailure{
				Stream:   "transitions",
				RunID:    rec.RunID,
				RecordID: rec.TransitionID,
				Reason:   canonicalErr.Error(),
			})
			expectedPrev = rec.EntryHash
			verified++
			continue
		}

		expectedEntry := entryHashFromCanonical(rec.PrevHash, canonical)
		if rec.EntryHash != expectedEntry {
			failures = append(failures, AuditIntegrityFailure{
				Stream:   "transitions",
				RunID:    rec.RunID,
				RecordID: rec.TransitionID,
				Reason:   fmt.Sprintf("entry_hash mismatch: expected %q got %q", expectedEntry, rec.EntryHash),
			})
		}

		expectedPrev = rec.EntryHash
		verified++
	}

	if err := rows.Err(); err != nil {
		return 0, nil, fmt.Errorf("iterate transition audit records: %w", err)
	}

	return verified, failures, nil
}

func verifyEventChain(ctx context.Context, db *sql.DB) (int, []AuditIntegrityFailure, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, created_at, prev_hash, entry_hash
		 FROM events
		 ORDER BY run_id ASC, run_event_seq ASC`,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("query events for audit verification: %w", err)
	}
	defer rows.Close()

	var (
		currentRunID string
		expectedPrev = AuditGenesisPrevHash
		expectedSeq  = 1
		verified     int
		failures     []AuditIntegrityFailure
	)

	for rows.Next() {
		var rec eventAuditRecord
		if err := rows.Scan(
			&rec.EventID,
			&rec.RunID,
			&rec.RunEventSeq,
			&rec.NodeRunID,
			&rec.AttemptID,
			&rec.SchemaVersion,
			&rec.EventType,
			&rec.Seq,
			&rec.PayloadJSON,
			&rec.ActorID,
			&rec.RoleID,
			&rec.AuthoritySource,
			&rec.ReasonCode,
			&rec.ReasonText,
			&rec.CorrelationID,
			&rec.CausationID,
			&rec.CreatedAt,
			&rec.PrevHash,
			&rec.EntryHash,
		); err != nil {
			return 0, nil, fmt.Errorf("scan event audit record: %w", err)
		}

		if rec.RunID != currentRunID {
			currentRunID = rec.RunID
			expectedPrev = AuditGenesisPrevHash
			expectedSeq = 1
		}

		if rec.RunEventSeq != expectedSeq {
			failures = append(failures, AuditIntegrityFailure{
				Stream:   "events",
				RunID:    rec.RunID,
				RecordID: rec.EventID,
				Reason:   fmt.Sprintf("run_event_seq gap: expected %d got %d", expectedSeq, rec.RunEventSeq),
			})
		}

		if rec.PrevHash != expectedPrev {
			failures = append(failures, AuditIntegrityFailure{
				Stream:   "events",
				RunID:    rec.RunID,
				RecordID: rec.EventID,
				Reason:   fmt.Sprintf("prev_hash mismatch: expected %q got %q", expectedPrev, rec.PrevHash),
			})
		}

		canonical, canonicalErr := canonicalEventJSON(rec)
		if canonicalErr != nil {
			failures = append(failures, AuditIntegrityFailure{
				Stream:   "events",
				RunID:    rec.RunID,
				RecordID: rec.EventID,
				Reason:   canonicalErr.Error(),
			})
			expectedPrev = rec.EntryHash
			verified++
			continue
		}

		expectedEntry := entryHashFromCanonical(rec.PrevHash, canonical)
		if rec.EntryHash != expectedEntry {
			failures = append(failures, AuditIntegrityFailure{
				Stream:   "events",
				RunID:    rec.RunID,
				RecordID: rec.EventID,
				Reason:   fmt.Sprintf("entry_hash mismatch: expected %q got %q", expectedEntry, rec.EntryHash),
			})
		}

		expectedPrev = rec.EntryHash
		expectedSeq = rec.RunEventSeq + 1
		verified++
	}

	if err := rows.Err(); err != nil {
		return 0, nil, fmt.Errorf("iterate event audit records: %w", err)
	}

	return verified, failures, nil
}

func loadPreviousTransitionHash(ctx context.Context, tx *sql.Tx, runID string, createdAt string, transitionID string) (string, error) {
	createdAtBefore := timestamporder.ComparisonSQL("created_at", "?", "<")
	createdAtEqual := timestamporder.ComparisonSQL("created_at", "?", "=")
	return loadPreviousHash(ctx, tx,
		`SELECT entry_hash
		 FROM transitions
		 WHERE run_id = ?
		   AND (`+createdAtBefore+` OR (`+createdAtEqual+` AND (created_at < ? OR (created_at = ? AND transition_id < ?))))
		 ORDER BY `+timestamporder.OrderBySQL("created_at", true)+`, transition_id DESC
		 LIMIT 1`,
		runID,
		createdAt,
		createdAt,
		createdAt,
		createdAt,
		transitionID,
	)
}

func loadPreviousEventHash(ctx context.Context, tx *sql.Tx, runID string, runEventSeq int) (string, error) {
	return loadPreviousHash(ctx, tx,
		`SELECT entry_hash
		 FROM events
		 WHERE run_id = ?
		   AND run_event_seq < ?
		 ORDER BY run_event_seq DESC
		 LIMIT 1`,
		runID,
		runEventSeq,
	)
}

func loadPreviousHash(ctx context.Context, tx *sql.Tx, query string, args ...any) (string, error) {
	var prevHash string
	err := tx.QueryRowContext(ctx, query, args...).Scan(&prevHash)
	if errors.Is(err, sql.ErrNoRows) {
		return AuditGenesisPrevHash, nil
	}
	if err != nil {
		return "", fmt.Errorf("load previous audit hash: %w", err)
	}
	if prevHash == "" {
		return "", fmt.Errorf("load previous audit hash: empty entry hash")
	}
	return prevHash, nil
}

func canonicalTransitionJSON(rec transitionAuditRecord) (string, error) {
	type canonicalTransition struct {
		ID              string `json:"id"`
		RunID           string `json:"run_id"`
		NodeRunID       string `json:"node_run_id,omitempty"`
		AttemptID       string `json:"attempt_id"`
		ActorID         string `json:"actor_id"`
		RoleID          string `json:"role_id"`
		AuthoritySource string `json:"authority_source"`
		ReasonCode      string `json:"reason_code"`
		ReasonText      string `json:"reason_text"`
		CorrelationID   string `json:"correlation_id"`
		CausationID     string `json:"causation_id"`
		CreatedAt       string `json:"created_at"`
		EntityType      string `json:"entity_type"`
		FromState       string `json:"from_state"`
		ToState         string `json:"to_state"`
	}

	encoded, err := json.Marshal(canonicalTransition{
		ID:              rec.TransitionID,
		RunID:           rec.RunID,
		NodeRunID:       strings.TrimSpace(rec.NodeRunID.String),
		AttemptID:       "",
		ActorID:         rec.ActorID,
		RoleID:          rec.RoleID,
		AuthoritySource: rec.AuthoritySource,
		ReasonCode:      rec.ReasonCode,
		ReasonText:      rec.ReasonText,
		CorrelationID:   rec.CorrelationID,
		CausationID:     rec.CausationID,
		CreatedAt:       rec.CreatedAt,
		EntityType:      rec.EntityType,
		FromState:       rec.FromState,
		ToState:         rec.ToState,
	})
	if err != nil {
		return "", fmt.Errorf("marshal canonical transition %q: %w", rec.TransitionID, err)
	}

	return string(encoded), nil
}

func canonicalEventJSON(rec eventAuditRecord) (string, error) {
	payload, err := normalizePayloadJSON(rec.PayloadJSON)
	if err != nil {
		return "", fmt.Errorf("canonicalize payload_json for event %q: %w", rec.EventID, err)
	}

	type canonicalEvent struct {
		ID              string          `json:"id"`
		RunID           string          `json:"run_id"`
		RunEventSeq     int             `json:"run_event_seq"`
		NodeRunID       string          `json:"node_run_id,omitempty"`
		AttemptID       string          `json:"attempt_id"`
		SchemaVersion   string          `json:"schema_version"`
		ActorID         string          `json:"actor_id"`
		RoleID          string          `json:"role_id"`
		AuthoritySource string          `json:"authority_source"`
		ReasonCode      string          `json:"reason_code"`
		ReasonText      string          `json:"reason_text"`
		CorrelationID   string          `json:"correlation_id"`
		CausationID     string          `json:"causation_id"`
		CreatedAt       string          `json:"created_at"`
		EventType       string          `json:"event_type"`
		Seq             int             `json:"seq"`
		PayloadJSON     json.RawMessage `json:"payload_json"`
	}

	encoded, err := json.Marshal(canonicalEvent{
		ID:              rec.EventID,
		RunID:           rec.RunID,
		RunEventSeq:     rec.RunEventSeq,
		NodeRunID:       strings.TrimSpace(rec.NodeRunID.String),
		AttemptID:       rec.AttemptID,
		SchemaVersion:   rec.SchemaVersion,
		ActorID:         rec.ActorID,
		RoleID:          rec.RoleID,
		AuthoritySource: rec.AuthoritySource,
		ReasonCode:      rec.ReasonCode,
		ReasonText:      rec.ReasonText,
		CorrelationID:   rec.CorrelationID,
		CausationID:     rec.CausationID,
		CreatedAt:       rec.CreatedAt,
		EventType:       rec.EventType,
		Seq:             rec.Seq,
		PayloadJSON:     payload,
	})
	if err != nil {
		return "", fmt.Errorf("marshal canonical event %q: %w", rec.EventID, err)
	}

	return string(encoded), nil
}

func normalizePayloadJSON(raw string) (json.RawMessage, error) {
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil, err
	}

	normalized, err := json.Marshal(decoded)
	if err != nil {
		return nil, err
	}

	return json.RawMessage(normalized), nil
}

func entryHashFromCanonical(prevHash string, canonical string) string {
	sum := sha256.Sum256([]byte(prevHash + "|" + canonical))
	return hex.EncodeToString(sum[:])
}
