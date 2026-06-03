package state

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestAuditChainPopulationAndVerification(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	transition := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypeNode, "running", "succeeded")
	req.TransitionID = "tr_audit_ok"
	req.EventID = "evt_audit_ok"
	req.NodeRunID = sql.NullString{String: "nr_1", Valid: true}
	req.AttemptID = "att_1"
	req.EventSeq = 2

	if err := transition.Apply(context.Background(), req); err != nil {
		t.Fatalf("apply transition: %v", err)
	}

	storedTransitionPrevHash, storedTransitionEntryHash := loadTransitionHashes(t, db, "tr_audit_ok")
	if storedTransitionPrevHash == "" || storedTransitionEntryHash == "" {
		t.Fatal("expected transition hashes to be populated")
	}
	if storedTransitionPrevHash == "prev_transition" || storedTransitionEntryHash == "entry_transition" {
		t.Fatal("expected computed transition hashes, not placeholder values")
	}

	storedEventPrevHash, storedEventEntryHash := loadEventHashes(t, db, "evt_audit_ok")
	if storedEventPrevHash == "" || storedEventEntryHash == "" {
		t.Fatal("expected event hashes to be populated")
	}
	if storedEventPrevHash == "prev_event" || storedEventEntryHash == "entry_event" {
		t.Fatal("expected computed event hashes, not placeholder values")
	}

	verifier := NewAuditIntegrityVerifier(db)
	if err := verifier.VerifyOrError(context.Background()); err != nil {
		t.Fatalf("verify audit chain: %v", err)
	}
}

func TestAuditChainCorruptionDetection(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	ingestor := NewAttemptEventIngestor(db, nil)
	_, err := ingestor.Ingest(context.Background(), AttemptEventIngestionRequest{
		EventID:         "evt_audit_corruption_2",
		RunID:           "run_1",
		NodeRunID:       sql.NullString{String: "nr_1", Valid: true},
		AttemptID:       "att_1",
		SchemaVersion:   "v1",
		EventType:       "attempt.progress",
		Seq:             2,
		PayloadJSON:     `{"message":"ok"}`,
		ActorID:         "actor_1",
		RoleID:          "king",
		AuthoritySource: "system",
		ReasonCode:      "internal_error",
		ReasonText:      "audit corruption setup",
		CorrelationID:   "corr_audit_2",
		CausationID:     "caus_audit_2",
		CreatedAt:       "2026-03-01T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("ingest event for corruption test: %v", err)
	}

	verifier := NewAuditIntegrityVerifier(db)
	if err := verifier.VerifyOrError(context.Background()); err != nil {
		t.Fatalf("verify audit chain before corruption: %v", err)
	}

	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_audit_corruption_tampered', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_1', 'att_1', 'v1', 'attempt.progress', 3, '{"message":"tampered"}', 'actor_1', 'king', 'system', 'internal_error', 'tampered append', 'corr_audit_tampered', 'caus_audit_tampered', '0000000000000000000000000000000000000000000000000000000000000000', '0000000000000000000000000000000000000000000000000000000000000000', '2026-03-01T12:02:00Z')`)

	err = verifier.VerifyOrError(context.Background())
	if err == nil {
		t.Fatal("expected integrity verification failure after tampering")
	}

	var integrityErr *AuditIntegrityError
	if !errors.As(err, &integrityErr) {
		t.Fatalf("expected AuditIntegrityError, got %T: %v", err, err)
	}
	if len(integrityErr.Failures) == 0 {
		t.Fatal("expected at least one integrity failure")
	}
}

func TestAuditChainDetectsTransitionNodeRunLineageTampering(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	transition := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypeNode, "running", "succeeded")
	req.TransitionID = "tr_audit_lineage_transition"
	req.EventID = "evt_audit_lineage_transition"
	req.NodeRunID = sql.NullString{String: "nr_1", Valid: true}
	req.AttemptID = "att_lineage_transition"
	req.EventType = "attempt.succeeded"
	if err := transition.Apply(context.Background(), req); err != nil {
		t.Fatalf("apply transition: %v", err)
	}

	verifier := NewAuditIntegrityVerifier(db)
	if err := verifier.VerifyOrError(context.Background()); err != nil {
		t.Fatalf("verify audit chain before tampering: %v", err)
	}

	mustExec(t, db, `DROP TRIGGER transitions_append_only_no_update`)
	mustExec(t, db, `UPDATE transitions SET node_run_id = NULL WHERE transition_id = 'tr_audit_lineage_transition'`)

	err := verifier.VerifyOrError(context.Background())
	if err == nil {
		t.Fatal("expected integrity verification failure after transition lineage tampering")
	}
}

func TestAuditChainDetectsEventLineageTampering(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	ingestor := NewAttemptEventIngestor(db, nil)
	_, err := ingestor.Ingest(context.Background(), AttemptEventIngestionRequest{
		EventID:         "evt_audit_lineage_event",
		RunID:           "run_1",
		NodeRunID:       sql.NullString{String: "nr_1", Valid: true},
		AttemptID:       "att_lineage_event",
		SchemaVersion:   "v1",
		EventType:       "attempt.progress",
		Seq:             2,
		PayloadJSON:     `{"message":"ok"}`,
		ActorID:         "actor_1",
		RoleID:          "king",
		AuthoritySource: "system",
		ReasonCode:      "internal_error",
		ReasonText:      "audit lineage tamper setup",
		CorrelationID:   "corr_lineage_event",
		CausationID:     "caus_lineage_event",
		CreatedAt:       "2026-03-01T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("ingest event: %v", err)
	}

	verifier := NewAuditIntegrityVerifier(db)
	if err := verifier.VerifyOrError(context.Background()); err != nil {
		t.Fatalf("verify audit chain before tampering: %v", err)
	}

	mustExec(t, db, `DROP TRIGGER events_append_only_no_update`)
	mustExec(t, db, `UPDATE events SET node_run_id = NULL, schema_version = 'v2' WHERE event_id = 'evt_audit_lineage_event'`)

	err = verifier.VerifyOrError(context.Background())
	if err == nil {
		t.Fatal("expected integrity verification failure after event lineage tampering")
	}
}

func TestAuditChainDetectsRunEventSequenceGaps(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	verifier := NewAuditIntegrityVerifier(db)
	if err := verifier.VerifyOrError(context.Background()); err != nil {
		t.Fatalf("verify audit chain before introducing gap: %v", err)
	}

	_, prevEntryHash := loadEventHashes(t, db, "evt_1")
	rec := eventAuditRecord{
		EventID:         "evt_audit_gap",
		RunID:           "run_1",
		RunEventSeq:     3,
		NodeRunID:       sql.NullString{String: "nr_1", Valid: true},
		AttemptID:       "att_gap",
		SchemaVersion:   "v1",
		EventType:       "attempt.progress",
		Seq:             1,
		PayloadJSON:     `{"message":"gap"}`,
		ActorID:         "actor_1",
		RoleID:          "king",
		AuthoritySource: "system",
		ReasonCode:      "internal_error",
		ReasonText:      "introduce sequence gap",
		CorrelationID:   "corr_gap",
		CausationID:     "caus_gap",
		CreatedAt:       "2026-03-01T12:02:00Z",
		PrevHash:        prevEntryHash,
	}
	canonical, err := canonicalEventJSON(rec)
	if err != nil {
		t.Fatalf("canonicalize gapped event: %v", err)
	}
	rec.EntryHash = entryHashFromCanonical(rec.PrevHash, canonical)

	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('`+rec.EventID+`', '`+rec.RunID+`', 3, 'nr_1', '`+rec.AttemptID+`', '`+rec.SchemaVersion+`', '`+rec.EventType+`', 1, '{"message":"gap"}', '`+rec.ActorID+`', '`+rec.RoleID+`', '`+rec.AuthoritySource+`', '`+rec.ReasonCode+`', '`+rec.ReasonText+`', '`+rec.CorrelationID+`', '`+rec.CausationID+`', '`+rec.PrevHash+`', '`+rec.EntryHash+`', '`+rec.CreatedAt+`')`)

	err = verifier.VerifyOrError(context.Background())
	if err == nil {
		t.Fatal("expected integrity verification failure after run_event_seq gap")
	}

	var integrityErr *AuditIntegrityError
	if !errors.As(err, &integrityErr) {
		t.Fatalf("expected AuditIntegrityError, got %T: %v", err, err)
	}
	if len(integrityErr.Failures) == 0 {
		t.Fatal("expected at least one integrity failure for sequence gap")
	}
}

func loadTransitionHashes(t *testing.T, db *sql.DB, transitionID string) (string, string) {
	t.Helper()

	var prevHash string
	var entryHash string
	if err := db.QueryRow(`SELECT prev_hash, entry_hash FROM transitions WHERE transition_id = ?`, transitionID).Scan(&prevHash, &entryHash); err != nil {
		t.Fatalf("query transition hashes: %v", err)
	}

	return prevHash, entryHash
}

func loadEventHashes(t *testing.T, db *sql.DB, eventID string) (string, string) {
	t.Helper()

	var prevHash string
	var entryHash string
	if err := db.QueryRow(`SELECT prev_hash, entry_hash FROM events WHERE event_id = ?`, eventID).Scan(&prevHash, &entryHash); err != nil {
		t.Fatalf("query event hashes: %v", err)
	}

	return prevHash, entryHash
}
