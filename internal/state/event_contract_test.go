package state

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	coreschemas "kingdom/internal/core/schemas"
)

func TestTransitionServiceRejectsEventEnvelopeThatViolatesSchema(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	svc := NewTransitionService(db, loadStateTestSchemas(t))
	req := baseTransitionRequest(EntityTypeRun, "running", "succeeded")
	req.TransitionID = "tr_invalid_event_contract"
	req.EventID = "evt_invalid_event_contract"
	req.AttemptID = "att_invalid_event_contract"
	req.EventType = "state.transitioned"
	req.PayloadJSON = `{"run_status":"succeeded"}`

	err := svc.Apply(context.Background(), req)
	if err == nil {
		t.Fatal("expected event contract validation failure")
	}

	var validationErr *EventContractValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected EventContractValidationError, got %T: %v", err, err)
	}
	if got := queryRunStatus(t, db, "run_1"); got != "running" {
		t.Fatalf("expected run status to remain running, got %q", got)
	}
	if got := queryTransitionCount(t, db, "run_1"); got != 0 {
		t.Fatalf("expected no transition rows after contract failure, got %d", got)
	}
	if got := queryEventExists(t, db, req.EventID); got {
		t.Fatalf("event %q should not have been inserted after contract failure", req.EventID)
	}
}

func TestAttemptEventIngestorRejectsEventEnvelopeThatViolatesSchema(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		eventID   string
		attemptID string
		eventType string
	}{
		{name: "unknown attempt event", eventID: "evt_invalid_ingested_event", attemptID: "att_invalid_ingested_event", eventType: "attempt.warning"},
		{name: "removed blocked event", eventID: "evt_removed_blocked_event", attemptID: "att_removed_blocked_event", eventType: "attempt.blocked"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)

			ingestor := NewAttemptEventIngestor(db, loadStateTestSchemas(t))
			req := baseAttemptEventIngestionRequest()
			req.EventID = tc.eventID
			req.AttemptID = tc.attemptID
			req.EventType = tc.eventType

			_, err := ingestor.Ingest(context.Background(), req)
			if err == nil {
				t.Fatal("expected event contract validation failure")
			}

			var validationErr *EventContractValidationError
			if !errors.As(err, &validationErr) {
				t.Fatalf("expected EventContractValidationError, got %T: %v", err, err)
			}
			if got := countAttemptEventsBySeq(t, db, req.AttemptID, req.Seq); got != 0 {
				t.Fatalf("expected no ingested events after contract failure, got %d", got)
			}
		})
	}
}

func TestEventSchemaRequiresRunEventSeqForPersistedEnvelope(t *testing.T) {
	t.Parallel()

	schemaSet := loadStateTestSchemas(t)

	withoutRunEventSeq := []byte(`{
		"event_id": "evt_missing_run_event_seq",
		"schema_version": "v1",
		"run_id": "run_1",
		"attempt_id": "att_1",
		"seq": 1,
		"created_at": "2026-03-01T12:00:00Z",
		"event_type": "attempt.started",
		"payload_json": {},
		"actor_id": "actor_1",
		"role_id": "governance",
		"authority_source": "system",
		"reason_code": "internal_error",
		"reason_text": "seed",
		"correlation_id": "corr_1",
		"causation_id": "caus_1"
	}`)
	if err := schemaSet.ValidateJSONBytes("event_v1", withoutRunEventSeq); err == nil {
		t.Fatal("expected persisted event schema to require run_event_seq")
	}

	withRunEventSeq := []byte(`{
		"event_id": "evt_with_run_event_seq",
		"schema_version": "v1",
		"run_id": "run_1",
		"run_event_seq": 1,
		"attempt_id": "att_1",
		"seq": 1,
		"created_at": "2026-03-01T12:00:00Z",
		"event_type": "attempt.started",
		"payload_json": {},
		"actor_id": "actor_1",
		"role_id": "governance",
		"authority_source": "system",
		"reason_code": "internal_error",
		"reason_text": "seed",
		"correlation_id": "corr_1",
		"causation_id": "caus_1"
	}`)
	if err := schemaSet.ValidateJSONBytes("event_v1", withRunEventSeq); err != nil {
		t.Fatalf("expected persisted event schema with run_event_seq to validate: %v", err)
	}
}

func loadStateTestSchemas(t *testing.T) *coreschemas.Set {
	t.Helper()

	schemaSet, err := coreschemas.Load(filepath.Join("..", "..", "contracts", "schemas"))
	if err != nil {
		t.Fatalf("load schemas: %v", err)
	}
	return schemaSet
}
