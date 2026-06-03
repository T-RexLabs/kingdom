package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/rolecomm"
	"kingdom/internal/core/schemas"
)

const (
	AttemptIngestGapEventType = "attempt.sequence_gap"
)

var terminalAttemptEventTypes = map[string]struct{}{
	"attempt.succeeded": {},
	"attempt.failed":    {},
	"attempt.cancelled": {},
}

type AttemptEventIngestionRequest struct {
	EventID         string
	RunID           string
	NodeRunID       sql.NullString
	AttemptID       string
	RunEventSeq     int
	SchemaVersion   string
	EventType       string
	Seq             int
	PayloadJSON     string
	ActorID         string
	RoleID          string
	AuthoritySource string
	ReasonCode      string
	ReasonText      string
	CorrelationID   string
	CausationID     string
	PrevHash        string
	EntryHash       string
	CreatedAt       string
}

type AttemptEventIngestionResult struct {
	Deduplicated bool
	GapDetected  bool
}

type attemptEventAppendOptions struct {
	mirrorToMailbox bool
}

type EventSequenceGapError struct {
	Code        string
	AttemptID   string
	ExpectedSeq int
	ObservedSeq int
}

func (e *EventSequenceGapError) Error() string {
	return fmt.Sprintf("%s: attempt=%q expected_seq=%d observed_seq=%d", e.Code, e.AttemptID, e.ExpectedSeq, e.ObservedSeq)
}

type MissingTerminalAttemptEventError struct {
	AttemptID     string
	LastEventType string
	LastEventSeq  int
}

func (e *MissingTerminalAttemptEventError) Error() string {
	if e.LastEventSeq == 0 {
		return fmt.Sprintf("missing terminal attempt event for %q: attempt has no events", e.AttemptID)
	}
	return fmt.Sprintf("missing terminal attempt event for %q: last event %q at seq=%d", e.AttemptID, e.LastEventType, e.LastEventSeq)
}

type AttemptEventIngestor struct {
	db                      *sql.DB
	eventSchemas            *schemas.Set
	roleCommunicationPolicy rolecomm.Policy
}

func NewAttemptEventIngestor(db *sql.DB, eventSchemas *schemas.Set) *AttemptEventIngestor {
	return &AttemptEventIngestor{db: db, eventSchemas: eventSchemas}
}

func (i *AttemptEventIngestor) WithRoleCommunicationPolicy(policy rolecomm.Policy) *AttemptEventIngestor {
	if i == nil {
		return nil
	}
	i.roleCommunicationPolicy = policy
	return i
}

func (i *AttemptEventIngestor) Ingest(ctx context.Context, req AttemptEventIngestionRequest) (AttemptEventIngestionResult, error) {
	if err := validateAttemptEventIngestionRequest(req); err != nil {
		return AttemptEventIngestionResult{}, err
	}

	tx, err := i.db.BeginTx(ctx, nil)
	if err != nil {
		return AttemptEventIngestionResult{}, fmt.Errorf("begin attempt event ingestion transaction: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	result := AttemptEventIngestionResult{}

	lastSeq, hasEvents, err := latestAttemptSequence(ctx, tx, req.AttemptID)
	if err != nil {
		return AttemptEventIngestionResult{}, err
	}

	if hasEvents && req.Seq <= lastSeq {
		exists, existsErr := attemptSequenceExists(ctx, tx, req.AttemptID, req.Seq)
		if existsErr != nil {
			return AttemptEventIngestionResult{}, existsErr
		}
		if exists {
			if err := tx.Commit(); err != nil {
				return AttemptEventIngestionResult{}, fmt.Errorf("commit dedupe ingestion transaction: %w", err)
			}
			committed = true
			return AttemptEventIngestionResult{Deduplicated: true}, nil
		}
		return AttemptEventIngestionResult{}, fmt.Errorf("attempt event sequence regression for %q: observed=%d latest=%d", req.AttemptID, req.Seq, lastSeq)
	}

	if hasEvents && req.Seq > lastSeq+1 {
		expectedSeq := lastSeq + 1
		gapEventReq := req
		gapEventReq.EventID = fmt.Sprintf("%s_gap_%d", req.AttemptID, expectedSeq)
		gapEventReq.EventType = AttemptIngestGapEventType
		gapEventReq.Seq = expectedSeq
		gapEventReq.PayloadJSON = fmt.Sprintf(`{"expected_seq":%d,"observed_seq":%d}`, expectedSeq, req.Seq)
		gapEventReq.ReasonCode = string(errorcatalog.CodeEventSequenceGap)
		gapEventReq.ReasonText = "attempt stream sequence gap detected"

		if err := appendAttemptEventTx(ctx, tx, i.eventSchemas, i.roleCommunicationPolicy, gapEventReq, attemptEventAppendOptions{mirrorToMailbox: true}); err != nil {
			return AttemptEventIngestionResult{}, err
		}

		result.GapDetected = true
	}

	if err := appendAttemptEventTx(ctx, tx, i.eventSchemas, i.roleCommunicationPolicy, req, attemptEventAppendOptions{mirrorToMailbox: true}); err != nil {
		if isAttemptEventDedupeConstraint(err) {
			if rollbackErr := rollbackAttemptEventDedupeConflict(ctx, i.db, tx, req.AttemptID, req.Seq); rollbackErr != nil {
				return AttemptEventIngestionResult{}, rollbackErr
			}
			committed = true
			result.Deduplicated = true
			return result, nil
		}
		return AttemptEventIngestionResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return AttemptEventIngestionResult{}, fmt.Errorf("commit attempt event ingestion transaction: %w", err)
	}
	committed = true

	if result.GapDetected {
		return result, &EventSequenceGapError{
			Code:        string(errorcatalog.CodeEventSequenceGap),
			AttemptID:   req.AttemptID,
			ExpectedSeq: lastSeq + 1,
			ObservedSeq: req.Seq,
		}
	}

	return result, nil
}

func (i *AttemptEventIngestor) EnsureTerminalEvent(ctx context.Context, attemptID string) error {
	if attemptID == "" {
		return fmt.Errorf("attempt id is required")
	}

	var eventType string
	var seq int
	err := i.db.QueryRowContext(ctx,
		`SELECT event_type, seq
		 FROM events
		 WHERE attempt_id = ?
		 ORDER BY seq DESC
		 LIMIT 1`,
		attemptID,
	).Scan(&eventType, &seq)
	if errors.Is(err, sql.ErrNoRows) {
		return &MissingTerminalAttemptEventError{AttemptID: attemptID}
	}
	if err != nil {
		return fmt.Errorf("query latest attempt event for %q: %w", attemptID, err)
	}

	if _, ok := terminalAttemptEventTypes[eventType]; ok {
		return nil
	}

	return &MissingTerminalAttemptEventError{
		AttemptID:     attemptID,
		LastEventType: eventType,
		LastEventSeq:  seq,
	}
}

func validateAttemptEventIngestionRequest(req AttemptEventIngestionRequest) error {
	if req.EventID == "" {
		return fmt.Errorf("event id is required")
	}
	if req.RunID == "" {
		return fmt.Errorf("run id is required")
	}
	if req.AttemptID == "" {
		return fmt.Errorf("attempt id is required")
	}
	if req.SchemaVersion == "" {
		return fmt.Errorf("schema version is required")
	}
	if req.EventType == "" {
		return fmt.Errorf("event type is required")
	}
	if req.Seq <= 0 {
		return fmt.Errorf("event sequence must be positive")
	}
	if req.PayloadJSON == "" {
		return fmt.Errorf("payload json is required")
	}
	if req.ActorID == "" {
		return fmt.Errorf("actor id is required")
	}
	if req.RoleID == "" {
		return fmt.Errorf("role id is required")
	}
	if req.AuthoritySource == "" {
		return fmt.Errorf("authority source is required")
	}
	if req.ReasonCode == "" {
		return fmt.Errorf("reason code is required")
	}
	if req.ReasonText == "" {
		return fmt.Errorf("reason text is required")
	}
	if req.CorrelationID == "" {
		return fmt.Errorf("correlation id is required")
	}
	if req.CausationID == "" {
		return fmt.Errorf("causation id is required")
	}
	if req.CreatedAt == "" {
		return fmt.Errorf("created timestamp is required")
	}

	return nil
}

func latestAttemptSequence(ctx context.Context, tx *sql.Tx, attemptID string) (int, bool, error) {
	var lastSeq int
	err := tx.QueryRowContext(ctx,
		`SELECT seq FROM events WHERE attempt_id = ? ORDER BY seq DESC LIMIT 1`,
		attemptID,
	).Scan(&lastSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("query latest sequence for attempt %q: %w", attemptID, err)
	}
	return lastSeq, true, nil
}

func attemptSequenceExists(ctx context.Context, tx *sql.Tx, attemptID string, seq int) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx,
		`SELECT 1 FROM events WHERE attempt_id = ? AND seq = ? LIMIT 1`,
		attemptID,
		seq,
	).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("query attempt sequence existence for attempt=%q seq=%d: %w", attemptID, seq, err)
	}
	return true, nil
}

func attemptSequenceExistsInDB(ctx context.Context, db *sql.DB, attemptID string, seq int) (bool, error) {
	var exists int
	err := db.QueryRowContext(ctx,
		`SELECT 1 FROM events WHERE attempt_id = ? AND seq = ? LIMIT 1`,
		attemptID,
		seq,
	).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("query attempt sequence existence for attempt=%q seq=%d: %w", attemptID, seq, err)
	}
	return true, nil
}

func rollbackAttemptEventDedupeConflict(ctx context.Context, db *sql.DB, tx *sql.Tx, attemptID string, seq int) error {
	if err := tx.Rollback(); err != nil {
		return fmt.Errorf("rollback dedupe ingestion transaction after unique conflict: %w", err)
	}
	exists, err := attemptSequenceExistsInDB(ctx, db, attemptID, seq)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("dedupe conflict for attempt=%q seq=%d did not resolve to an existing persisted event", attemptID, seq)
	}
	return nil
}

func appendAttemptEventTx(ctx context.Context, tx *sql.Tx, schemaSet *schemas.Set, policy rolecomm.Policy, req AttemptEventIngestionRequest, options attemptEventAppendOptions) error {
	if err := validateAttemptEventIngestionRequest(req); err != nil {
		return err
	}
	if req.RunEventSeq <= 0 {
		runEventSeq, err := nextRunEventSeqTx(ctx, tx, req.RunID)
		if err != nil {
			return fmt.Errorf("allocate run event sequence: %w", err)
		}
		req.RunEventSeq = runEventSeq
	}
	if err := validateAttemptEventContract(schemaSet, req); err != nil {
		return err
	}
	if err := assignIngestionEventHashes(ctx, tx, &req); err != nil {
		return fmt.Errorf("assign event audit hashes: %w", err)
	}

	_, err := tx.ExecContext(ctx,
		`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.EventID,
		req.RunID,
		req.RunEventSeq,
		req.NodeRunID,
		req.AttemptID,
		req.SchemaVersion,
		req.EventType,
		req.Seq,
		req.PayloadJSON,
		req.ActorID,
		req.RoleID,
		req.AuthoritySource,
		req.ReasonCode,
		req.ReasonText,
		req.CorrelationID,
		req.CausationID,
		req.PrevHash,
		req.EntryHash,
		req.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert attempt event %q: %w", req.EventID, err)
	}
	runActivityChanged, err := AdvanceRunActivityTx(ctx, tx, RunActivityUpdateRequest{
		EventID:     req.EventID,
		EventType:   req.EventType,
		OccurredAt:  req.CreatedAt,
		PayloadJSON: req.PayloadJSON,
		ReasonCode:  req.ReasonCode,
		RunEventSeq: req.RunEventSeq,
		RunID:       req.RunID,
	})
	if err != nil {
		return fmt.Errorf("advance run activity: %w", err)
	}
	shouldProject := ShouldProjectRunCollectionEvent(req.EventType)
	if shouldProject && runActivityChanged {
		if err := AppendRunCollectionEventTx(ctx, tx, RunCollectionEventAppendRequest{
			EventID:     req.EventID,
			RunID:       req.RunID,
			EventType:   req.EventType,
			PayloadJSON: req.PayloadJSON,
			CreatedAt:   req.CreatedAt,
		}); err != nil {
			return err
		}
	}

	if options.mirrorToMailbox && shouldMirrorAttemptEventToMailbox(req) {
		_ = MirrorEventToMailbox(ctx, tx, policy, EventMailboxMirrorRequest{
			EventID:     req.EventID,
			RunID:       req.RunID,
			NodeRunID:   req.NodeRunID,
			AttemptID:   req.AttemptID,
			EventType:   req.EventType,
			PayloadJSON: req.PayloadJSON,
			RoleID:      req.RoleID,
			ReasonCode:  req.ReasonCode,
			ReasonText:  req.ReasonText,
			CreatedAt:   req.CreatedAt,
		})
	}

	return nil
}

func shouldMirrorAttemptEventToMailbox(req AttemptEventIngestionRequest) bool {
	code := strings.TrimSpace(strings.ToLower(req.ReasonCode))
	switch code {
	case "runtime_observation", "runtime_completion_observation":
		return false
	default:
		return true
	}
}

func isAttemptEventDedupeConstraint(err error) bool {
	return IsUniqueViolationOn(err, "events.attempt_id", "events.seq")
}
