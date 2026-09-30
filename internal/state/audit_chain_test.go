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

func TestAuditChainOrdersMixedRFC3339TimestampPrecisionByInstant(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	transition := NewTransitionService(db, nil)
	first := baseTransitionRequest(EntityTypeRun, "running", "awaiting_human_confirmation")
	first.TransitionID = "tr_whole_second"
	first.EventID = "evt_whole_second"
	first.AttemptID = "att_whole_second"
	first.EventSeq = 1
	first.OccurredAt = "2026-03-01T12:30:00Z"
	if err := transition.Apply(context.Background(), first); err != nil {
		t.Fatalf("apply whole-second transition: %v", err)
	}

	second := baseTransitionRequest(EntityTypeRun, "awaiting_human_confirmation", "succeeded")
	second.TransitionID = "tr_fractional_second"
	second.EventID = "evt_fractional_second"
	second.AttemptID = "att_fractional_second"
	second.EventSeq = 1
	second.OccurredAt = "2026-03-01T12:30:00.100000000Z"
	if err := transition.Apply(context.Background(), second); err != nil {
		t.Fatalf("apply fractional-second transition: %v", err)
	}

	if err := NewAuditIntegrityVerifier(db).VerifyOrError(context.Background()); err != nil {
		t.Fatalf("verify chain after transitions with mixed RFC3339 precision: %v", err)
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
		RoleID:          "governance",
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

	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_audit_corruption_tampered', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_1', 'att_1', 'v1', 'attempt.progress', 3, '{"message":"tampered"}', 'actor_1', 'governance', 'system', 'internal_error', 'tampered append', 'corr_audit_tampered', 'caus_audit_tampered', '0000000000000000000000000000000000000000000000000000000000000000', '0000000000000000000000000000000000000000000000000000000000000000', '2026-03-01T12:02:00Z')`)

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
		RoleID:          "governance",
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
		RoleID:          "governance",
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

func TestAuditChainVerifiesAndDetectsExternalIntegrationTampering(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	_, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-audit", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-audit", ActorID: "local-operator", CreatedAt: "2026-08-12T10:00:00Z", AuditEventID: "external-audit-1"})
	if err != nil {
		t.Fatal(err)
	}
	name := "Renamed"
	if _, err := store.UpdateConnection(ctx, "conn-audit", UpdateExternalConnectionRequest{DisplayName: &name, ActorID: "local-operator", UpdatedAt: "2026-08-12T11:00:00Z", AuditEventID: "external-audit-2"}); err != nil {
		t.Fatal(err)
	}
	var firstSequence, secondSequence, headSequence int
	var firstPrevious, secondHash, headHash, hashVersion string
	if err := db.QueryRow(`SELECT event_sequence,previous_hash,hash_version FROM external_integration_audit_events WHERE audit_event_id='external-audit-1'`).Scan(&firstSequence, &firstPrevious, &hashVersion); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT event_sequence,event_hash FROM external_integration_audit_events WHERE audit_event_id='external-audit-2'`).Scan(&secondSequence, &secondHash); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT last_sequence,last_hash FROM external_integration_audit_head WHERE head_id=1`).Scan(&headSequence, &headHash); err != nil {
		t.Fatal(err)
	}
	if firstSequence != 1 || secondSequence != 2 || firstPrevious != AuditGenesisPrevHash || hashVersion != ExternalIntegrationAuditHashVersion || headSequence != 2 || headHash != secondHash {
		t.Fatalf("invalid external audit allocation: first=(%d,%q,%q) second=%d head=(%d,%q) second_hash=%q", firstSequence, firstPrevious, hashVersion, secondSequence, headSequence, headHash, secondHash)
	}
	verifier := NewAuditIntegrityVerifier(db)
	result, err := verifier.Verify(ctx)
	if err != nil || len(result.Failures) != 0 || result.ExternalIntegrationEventsVerified != 2 {
		t.Fatalf("external chain verification: result=%#v err=%v", result, err)
	}
	mustExec(t, db, `DROP TRIGGER external_audit_no_update`)
	mustExec(t, db, `UPDATE external_integration_audit_events SET safe_payload_json='{"tampered":true}' WHERE audit_event_id='external-audit-1'`)
	err = verifier.VerifyOrError(ctx)
	var integrityErr *AuditIntegrityError
	if !errors.As(err, &integrityErr) {
		t.Fatalf("expected external audit integrity error, got %v", err)
	}
	found := false
	for _, failure := range integrityErr.Failures {
		if failure.Stream == "external_integration_audit_events" && failure.RecordID == "external-audit-1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing external audit failure: %#v", integrityErr.Failures)
	}
}

func TestAuditChainDetectsExternalSequenceVersionAndHeadTampering(t *testing.T) {
	for _, test := range []struct {
		name   string
		tamper func(*testing.T, *sql.DB)
		stream string
	}{
		{
			name: "sequence gap",
			tamper: func(t *testing.T, db *sql.DB) {
				mustExec(t, db, `DROP TRIGGER external_audit_no_update`)
				mustExec(t, db, `UPDATE external_integration_audit_events SET event_sequence=2 WHERE audit_event_id='external-audit-tamper'`)
			},
			stream: "external_integration_audit_events",
		},
		{
			name: "unsupported hash version",
			tamper: func(t *testing.T, db *sql.DB) {
				mustExec(t, db, `DROP TRIGGER external_audit_no_update`)
				mustExec(t, db, `PRAGMA ignore_check_constraints=ON`)
				mustExec(t, db, `UPDATE external_integration_audit_events SET hash_version='v2' WHERE audit_event_id='external-audit-tamper'`)
				mustExec(t, db, `PRAGMA ignore_check_constraints=OFF`)
			},
			stream: "external_integration_audit_events",
		},
		{
			name: "durable head mismatch",
			tamper: func(t *testing.T, db *sql.DB) {
				mustExec(t, db, `DROP TRIGGER external_audit_head_fenced`)
				mustExec(t, db, `UPDATE external_integration_audit_head SET last_sequence=0,last_hash='0000000000000000000000000000000000000000000000000000000000000000' WHERE head_id=1`)
			},
			stream: "external_integration_audit_head",
		},
		{
			name: "durable head missing",
			tamper: func(t *testing.T, db *sql.DB) {
				mustExec(t, db, `DROP TRIGGER external_audit_head_no_delete`)
				mustExec(t, db, `DELETE FROM external_integration_audit_head WHERE head_id=1`)
			},
			stream: "external_integration_audit_head",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			store := NewExternalStore(db)
			ctx := context.Background()
			if _, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-audit-tamper", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-audit-tamper", ActorID: "local-operator", CreatedAt: "2026-08-12T10:00:00Z", AuditEventID: "external-audit-tamper"}); err != nil {
				t.Fatal(err)
			}
			test.tamper(t, db)
			result, err := NewAuditIntegrityVerifier(db).Verify(ctx)
			if err != nil {
				t.Fatalf("verify tampered chain: %v", err)
			}
			found := false
			for _, failure := range result.Failures {
				if failure.Stream == test.stream {
					found = true
				}
			}
			if !found {
				t.Fatalf("missing %s failure: %#v", test.stream, result.Failures)
			}
		})
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
