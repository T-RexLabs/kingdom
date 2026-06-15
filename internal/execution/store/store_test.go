package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"kingdom/internal/dbmigrations"
)

func TestStoreEnqueueAttemptIdempotent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	req := EnqueueAttemptRequest{
		AttemptID:  "run_1:decree_intake:1",
		RunID:      "run_1",
		NodeRunID:  "nr_1",
		SessionRef: "session_run_1_decree_intake_1",
		CreatedAt:  "2026-03-01T12:00:01Z",
	}

	if err := s.EnqueueAttempt(ctx, req); err != nil {
		t.Fatalf("enqueue attempt first call: %v", err)
	}
	if err := s.EnqueueAttempt(ctx, req); err != nil {
		t.Fatalf("enqueue attempt second call: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_executions WHERE attempt_id = ?`, req.AttemptID).Scan(&count); err != nil {
		t.Fatalf("query attempt row count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected idempotent enqueue to keep one row, got %d", count)
	}
}

func TestStoreEnqueueAttemptPersistsExecutionMode(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:     "run_1:decree_intake:1",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		ExecutionMode: ExecutionModeRuntimeOwned,
		CreatedAt:     "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue runtime-owned attempt: %v", err)
	}

	attempt, ok, err := s.GetAttempt(ctx, "run_1:decree_intake:1")
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !ok {
		t.Fatal("expected attempt to exist")
	}
	if attempt.ExecutionMode != ExecutionModeRuntimeOwned {
		t.Fatalf("expected execution_mode=%q, got %q", ExecutionModeRuntimeOwned, attempt.ExecutionMode)
	}
	if attempt.SessionRef != "" {
		t.Fatalf("expected empty session_ref for runtime-owned attempt, got %q", attempt.SessionRef)
	}
	if attempt.ProviderSessionID != "" {
		t.Fatalf("expected empty provider_session_id for runtime-owned attempt, got %q", attempt.ProviderSessionID)
	}
}

func TestStoreUpdateProviderSessionIDRoundTripsOnAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:  attemptID,
		RunID:      "run_1",
		NodeRunID:  "nr_1",
		SessionRef: "session_run_1_decree_intake_1",
		CreatedAt:  "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := s.UpdateProviderSessionID(ctx, UpdateProviderSessionIDRequest{
		AttemptID:         attemptID,
		ProviderSessionID: "ses_real_123",
		UpdatedAt:         "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("update provider session id: %v", err)
	}

	attempt, ok, err := s.GetAttempt(ctx, attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !ok {
		t.Fatal("expected attempt to exist")
	}
	if attempt.ProviderSessionID != "ses_real_123" {
		t.Fatalf("expected provider_session_id ses_real_123, got %q", attempt.ProviderSessionID)
	}

	var updatedAt string
	if err := db.QueryRow(`SELECT updated_at FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&updatedAt); err != nil {
		t.Fatalf("query attempt updated_at: %v", err)
	}
	if updatedAt != "2026-03-01T12:00:01Z" {
		t.Fatalf("expected metadata write to preserve execution updated_at, got %q", updatedAt)
	}
}

func TestStoreUpdateProviderSessionIDDoesNotReorderRunExecutions(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:01:10Z', NULL)`)

	s := New(db)
	ctx := context.Background()
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:  "run_1:decree_intake:1",
		RunID:      "run_1",
		NodeRunID:  "nr_1",
		SessionRef: "session_decree_1",
		CreatedAt:  "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("enqueue first attempt: %v", err)
	}
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:  "run_1:decree_intake:2",
		RunID:      "run_1",
		NodeRunID:  "nr_2",
		SessionRef: "session_decree_2",
		CreatedAt:  "2026-03-01T12:01:00Z",
	}); err != nil {
		t.Fatalf("enqueue second attempt: %v", err)
	}
	if acquired, err := s.AcquireLease(ctx, AcquireLeaseRequest{
		AttemptID:    "run_1:decree_intake:1",
		LeaseOwner:   "worker-1",
		LeaseUntil:   "2026-03-01T12:20:00Z",
		AcquiredAt:   "2026-03-01T12:00:10Z",
		StateOnLease: "running",
	}); err != nil || !acquired {
		t.Fatalf("acquire first lease: acquired=%t err=%v", acquired, err)
	}
	if acquired, err := s.AcquireLease(ctx, AcquireLeaseRequest{
		AttemptID:    "run_1:decree_intake:2",
		LeaseOwner:   "worker-2",
		LeaseUntil:   "2026-03-01T12:20:00Z",
		AcquiredAt:   "2026-03-01T12:01:10Z",
		StateOnLease: "running",
	}); err != nil || !acquired {
		t.Fatalf("acquire second lease: acquired=%t err=%v", acquired, err)
	}
	if err := s.UpdateProviderSessionID(ctx, UpdateProviderSessionIDRequest{
		AttemptID:         "run_1:decree_intake:1",
		ProviderSessionID: "ses_decree_1_real",
		UpdatedAt:         "2026-03-01T12:02:00Z",
	}); err != nil {
		t.Fatalf("late provider session update: %v", err)
	}

	records, err := s.ListRunExecutions(ctx, "run_1")
	if err != nil {
		t.Fatalf("list run executions: %v", err)
	}
	if len(records) < 2 {
		t.Fatalf("expected at least 2 executions, got %#v", records)
	}
	if records[0].AttemptID != "run_1:decree_intake:2" {
		t.Fatalf("expected newer execution activity to remain first, got %#v", records)
	}
	if records[0].ActivityAt != "2026-03-01T12:01:10Z" {
		t.Fatalf("expected newer execution activity_at to remain unchanged, got %q", records[0].ActivityAt)
	}
}

func TestStoreListRunExecutionsReturnsExecutionScopedSessionIdentity(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder Berserker', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'builder-berserker', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder', 'run_1', 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent_builder', 1, NULL, NULL)`)

	s := New(db)
	ctx := context.Background()
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:     "run_1:decree_intake:1",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		ExecutionMode: ExecutionModeRuntimeOwned,
		CreatedAt:     "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue runtime-owned attempt: %v", err)
	}
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:         "run_1:delegated_execution_berserker:1",
		RunID:             "run_1",
		NodeRunID:         "nr_builder",
		ExecutionMode:     ExecutionModeProviderBacked,
		SessionRef:        "session_builder_run_1",
		ProviderSessionID: "ses_builder_run_1",
		CreatedAt:         "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("enqueue provider-backed attempt: %v", err)
	}
	acquired, err := s.AcquireLease(ctx, AcquireLeaseRequest{
		AttemptID:    "run_1:delegated_execution_berserker:1",
		LeaseOwner:   "worker-builder",
		LeaseUntil:   "2026-03-01T12:20:00Z",
		AcquiredAt:   "2026-03-01T12:00:03Z",
		StateOnLease: "running",
	})
	if err != nil {
		t.Fatalf("acquire provider-backed lease: %v", err)
	}
	if !acquired {
		t.Fatal("expected provider-backed lease acquire to succeed")
	}

	records, err := s.ListRunExecutions(ctx, "run_1")
	if err != nil {
		t.Fatalf("list run executions: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 run executions, got %d", len(records))
	}

	providerBacked := records[0]
	if providerBacked.AttemptID != "run_1:delegated_execution_berserker:1" {
		t.Fatalf("expected provider-backed attempt first, got %#v", providerBacked)
	}
	if providerBacked.NodeID != "delegated_execution_berserker" || providerBacked.Attempt != 1 {
		t.Fatalf("unexpected provider-backed node lineage: %#v", providerBacked)
	}
	if providerBacked.ExecutionMode != ExecutionModeProviderBacked {
		t.Fatalf("expected provider_backed execution mode, got %q", providerBacked.ExecutionMode)
	}
	if !providerBacked.LogicalSessionID.Valid || providerBacked.LogicalSessionID.String != "session_builder_run_1" {
		t.Fatalf("expected logical session id, got %#v", providerBacked.LogicalSessionID)
	}
	if !providerBacked.ProviderSessionID.Valid || providerBacked.ProviderSessionID.String != "ses_builder_run_1" {
		t.Fatalf("expected provider session id, got %#v", providerBacked.ProviderSessionID)
	}
	if providerBacked.ActivityAt != "2026-03-01T12:00:03Z" {
		t.Fatalf("expected provider-backed activity_at from latest execution activity, got %q", providerBacked.ActivityAt)
	}
	if providerBacked.QueuedAt != "2026-03-01T12:00:02Z" {
		t.Fatalf("expected provider-backed queued_at from attempt creation, got %q", providerBacked.QueuedAt)
	}

	runtimeOwned := records[1]
	if runtimeOwned.AttemptID != "run_1:decree_intake:1" {
		t.Fatalf("expected runtime-owned attempt second, got %#v", runtimeOwned)
	}
	if runtimeOwned.ExecutionMode != ExecutionModeRuntimeOwned {
		t.Fatalf("expected runtime_owned execution mode, got %q", runtimeOwned.ExecutionMode)
	}
	if runtimeOwned.LogicalSessionID.Valid {
		t.Fatalf("expected runtime-owned attempt to omit logical session id, got %#v", runtimeOwned.LogicalSessionID)
	}
	if runtimeOwned.ProviderSessionID.Valid {
		t.Fatalf("expected runtime-owned attempt to omit provider session id, got %#v", runtimeOwned.ProviderSessionID)
	}
	if runtimeOwned.ActivityAt != "2026-03-01T12:00:01Z" {
		t.Fatalf("expected runtime-owned activity_at from created execution row, got %q", runtimeOwned.ActivityAt)
	}
	if runtimeOwned.QueuedAt != "2026-03-01T12:00:01Z" {
		t.Fatalf("expected runtime-owned queued_at from attempt creation, got %q", runtimeOwned.QueuedAt)
	}
}

func TestStoreListRunExecutionsOrdersByActivityAt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:01:10Z', NULL)`)

	s := New(db)
	ctx := context.Background()
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:         "run_1:decree_intake:1",
		RunID:             "run_1",
		NodeRunID:         "nr_1",
		ExecutionMode:     ExecutionModeProviderBacked,
		SessionRef:        "session_decree_1",
		ProviderSessionID: "ses_decree_1",
		CreatedAt:         "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("enqueue first attempt: %v", err)
	}
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:         "run_1:decree_intake:2",
		RunID:             "run_1",
		NodeRunID:         "nr_2",
		ExecutionMode:     ExecutionModeProviderBacked,
		SessionRef:        "session_decree_2",
		ProviderSessionID: "ses_decree_2",
		CreatedAt:         "2026-03-01T12:01:00Z",
	}); err != nil {
		t.Fatalf("enqueue second attempt: %v", err)
	}
	if acquired, err := s.AcquireLease(ctx, AcquireLeaseRequest{
		AttemptID:    "run_1:decree_intake:1",
		LeaseOwner:   "worker-1",
		LeaseUntil:   "2026-03-01T12:20:00Z",
		AcquiredAt:   "2026-03-01T12:00:10Z",
		StateOnLease: "running",
	}); err != nil || !acquired {
		t.Fatalf("acquire first lease: acquired=%t err=%v", acquired, err)
	}
	if acquired, err := s.AcquireLease(ctx, AcquireLeaseRequest{
		AttemptID:    "run_1:decree_intake:2",
		LeaseOwner:   "worker-2",
		LeaseUntil:   "2026-03-01T12:20:00Z",
		AcquiredAt:   "2026-03-01T12:01:10Z",
		StateOnLease: "running",
	}); err != nil || !acquired {
		t.Fatalf("acquire second lease: acquired=%t err=%v", acquired, err)
	}
	mustExec(t, db, `UPDATE attempt_executions
		SET state = 'committed_success',
		    terminal_observed_at = '2026-03-01T12:05:00Z',
		    committed_at = '2026-03-01T12:05:00Z',
		    updated_at = '2026-03-01T12:05:00Z'
		WHERE attempt_id = 'run_1:decree_intake:1'`)

	records, err := s.ListRunExecutions(ctx, "run_1")
	if err != nil {
		t.Fatalf("list run executions: %v", err)
	}
	if len(records) < 2 {
		t.Fatalf("expected at least 2 run executions, got %d", len(records))
	}
	if records[0].AttemptID != "run_1:decree_intake:1" {
		t.Fatalf("expected attempt with latest activity first, got %#v", records[0])
	}
	if records[0].ActivityAt != "2026-03-01T12:05:00Z" {
		t.Fatalf("expected first activity_at to reflect commit time, got %q", records[0].ActivityAt)
	}
}

func TestStoreGetRunExecutionDetailIncludesCanonicalSettlementProjection(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer Shieldmaiden', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer', 'reviewer-shieldmaiden', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review', 'run_1', 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:05:00Z')`)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:execution_review_shieldmaiden:1"
	seedCanonicalProviderExecutionOutput(t, db, attemptID, "run_1", "nr_review", "committed_failure", "execution_review_rework", "review requested rework", "VERDICT: REWORK\n\nCanonical reviewer response.", "2026-03-01T12:00:00Z")
	mustExec(t, db, `UPDATE attempt_executions
		SET state = 'committed_failure',
		    terminal_observed_at = '2026-03-01T12:05:00Z',
		    committed_at = '2026-03-01T12:05:00Z',
		    failure_code = 'execution_review_rework',
		    failure_text = 'review requested rework',
		    updated_at = '2026-03-01T12:05:00Z'
		WHERE attempt_id = 'run_1:execution_review_shieldmaiden:1'`)
	if err := s.QueueOutbox(ctx, QueueOutboxRequest{
		OutboxID:      "outbox_review_detail",
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_review",
		TerminalSeq:   1,
		TerminalState: "committed_failure",
		PayloadJSON:   `{"provider_invocation_id":"inv_review_stale","provider_invocation_at":"2026-03-01T12:00:01Z","output_text":"VERDICT: PASS\n\nStale reviewer response."}`,
		CreatedAt:     "2026-03-01T12:05:00Z",
	}); err != nil {
		t.Fatalf("queue outbox: %v", err)
	}

	record, found, err := s.GetRunExecutionDetail(ctx, "run_1", attemptID)
	if err != nil {
		t.Fatalf("get run execution detail: %v", err)
	}
	if !found {
		t.Fatal("expected run execution detail to be found")
	}
	if record.AttemptID != attemptID || record.NodeID != "execution_review_shieldmaiden" {
		t.Fatalf("unexpected execution detail lineage: %#v", record)
	}
	if !record.ProviderInvocationID.Valid || record.ProviderInvocationID.String != "pinv_run_1_execution_review_shieldmaiden_1" {
		t.Fatalf("expected provider invocation id, got %#v", record.ProviderInvocationID)
	}
	if record.QueuedAt != "2026-03-01T12:00:00Z" {
		t.Fatalf("expected queued_at from attempt creation, got %q", record.QueuedAt)
	}
	if !record.ProviderInvocationAt.Valid || record.ProviderInvocationAt.String != "2026-03-01T12:00:00Z" {
		t.Fatalf("expected provider invocation at, got %#v", record.ProviderInvocationAt)
	}
	if !record.ObservedOutputText.Valid || record.ObservedOutputText.String != "VERDICT: REWORK\n\nCanonical reviewer response." {
		t.Fatalf("expected observed output text, got %#v", record.ObservedOutputText)
	}
	if !record.ObservedControlVerdict.Valid || record.ObservedControlVerdict.String != "REWORK" {
		t.Fatalf("expected observed control verdict REWORK, got %#v", record.ObservedControlVerdict)
	}
	if !record.CanonicalSettlementID.Valid || record.CanonicalSettlementID.String != "settlement_run_1_execution_review_shieldmaiden_1" {
		t.Fatalf("expected canonical settlement id, got %#v", record.CanonicalSettlementID)
	}
	if !record.CanonicalSettlementOutcome.Valid || record.CanonicalSettlementOutcome.String != "failed" {
		t.Fatalf("expected canonical settlement outcome failed, got %#v", record.CanonicalSettlementOutcome)
	}
	if !record.CanonicalSettlementAt.Valid || record.CanonicalSettlementAt.String != "2026-03-01T12:00:00Z" {
		t.Fatalf("expected canonical settlement timestamp, got %#v", record.CanonicalSettlementAt)
	}
	if !record.CanonicalStructuredPayloadJSON.Valid || record.CanonicalStructuredPayloadJSON.String != `{"verdict":"REWORK"}` {
		t.Fatalf("expected canonical structured payload, got %#v", record.CanonicalStructuredPayloadJSON)
	}
}

func TestStoreGetRunExecutionDetailReturnsFalseForUnknownAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	record, found, err := s.GetRunExecutionDetail(context.Background(), "run_1", "run_1:missing:1")
	if err != nil {
		t.Fatalf("get run execution detail: %v", err)
	}
	if found {
		t.Fatalf("expected missing attempt to return found=false, got %#v", record)
	}
}

func TestStoreListLatestRunExecutionOutputsUsesLatestCommittedAttemptPerNode(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer Shieldmaiden', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at, display_name) VALUES ('agent_reviewer', 'reviewer-shieldmaiden', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', 'Reviewer Shieldmaiden')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at, display_name) VALUES ('agent_validator', 'intelligence-qa-scout', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', 'QA Scout')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review', 'wfdef_1', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation', 'wfdef_1', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_1', 'run_1', 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:01:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_2', 'run_1', 'execution_review_shieldmaiden', 2,2, 'succeeded', 'reviewer-shieldmaiden', 'agent_reviewer', 1, '2026-03-01T12:02:00Z', '2026-03-01T12:03:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_1', 'run_1', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validator', 1, '2026-03-01T12:04:00Z', '2026-03-01T12:05:00Z')`)

	s := New(db)
	ctx := context.Background()
	seedOutput := func(attemptID, nodeRunID, state, failureCode, failureText, reasonCode, outputText, createdAt string) {
		t.Helper()
		seedCanonicalProviderExecutionOutput(t, db, attemptID, "run_1", nodeRunID, state, failureCode, failureText, outputText, createdAt)
		if _, err := db.Exec(`UPDATE attempt_executions
			SET state = ?,
			    terminal_observed_at = ?,
			    committed_at = ?,
			    failure_code = ?,
			    failure_text = ?,
		    updated_at = ?
			WHERE attempt_id = ?`, state, createdAt, createdAt, failureCode, failureText, createdAt, attemptID); err != nil {
			t.Fatalf("update attempt %s: %v", attemptID, err)
		}
		payloadJSON := fmt.Sprintf(`{"attempt_id":%q,"output_text":%q,"outcome":{"reason_code":%q}}`, attemptID, "VERDICT: PASS\n\nStale outbox payload.", reasonCode)
		if err := s.QueueOutbox(ctx, QueueOutboxRequest{OutboxID: "outbox_" + strings.ReplaceAll(nodeRunID, ":", "_"), AttemptID: attemptID, RunID: "run_1", NodeRunID: nodeRunID, TerminalSeq: 1, TerminalState: state, PayloadJSON: payloadJSON, CreatedAt: createdAt}); err != nil {
			t.Fatalf("queue outbox %s: %v", attemptID, err)
		}
	}
	seedOutput("run_1:execution_review_shieldmaiden:1", "nr_review_1", "committed_failure", "execution_review_rework", "review requested rework", "execution_review_rework", "VERDICT: REWORK\n\nOld review failure.", "2026-03-01T12:01:00Z")
	seedOutput("run_1:execution_review_shieldmaiden:2", "nr_review_2", "committed_success", "", "", "execution_completed", "VERDICT: PASS\n\nLatest review pass.", "2026-03-01T12:03:00Z")
	seedOutput("run_1:validation_gates_qa_scout:1", "nr_validation_1", "committed_failure", "validation_gate_rework", "validation requested rework", "validation_gate_rework", "VERDICT: REWORK\n\nLatest validator failure.", "2026-03-01T12:05:00Z")

	records, err := s.ListLatestRunExecutionOutputs(ctx, ListRunExecutionOutputsRequest{
		RunID:       "run_1",
		NodeClasses: []string{"execution_review", "validation_gates"},
		States:      []string{"committed_success", "committed_failure"},
	})
	if err != nil {
		t.Fatalf("list latest run execution outputs: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected two latest output records, got %#v", records)
	}
	if records[0].NodeID != "execution_review_shieldmaiden" || records[0].AttemptID != "run_1:execution_review_shieldmaiden:2" {
		t.Fatalf("expected latest review output record, got %#v", records[0])
	}
	if !records[0].OutputText.Valid || records[0].OutputText.String != "VERDICT: PASS\n\nLatest review pass." {
		t.Fatalf("expected latest review output text, got %#v", records[0].OutputText)
	}
	if !records[0].OutcomeReasonCode.Valid || records[0].OutcomeReasonCode.String != "execution_completed" {
		t.Fatalf("expected latest review reason code, got %#v", records[0].OutcomeReasonCode)
	}
	if !records[0].AgentDisplayName.Valid || records[0].AgentDisplayName.String != "Reviewer Shieldmaiden" {
		t.Fatalf("expected latest review agent display name, got %#v", records[0].AgentDisplayName)
	}
	if records[1].NodeID != "validation_gates_qa_scout" || records[1].AttemptID != "run_1:validation_gates_qa_scout:1" {
		t.Fatalf("expected latest validator output record, got %#v", records[1])
	}
	if !records[1].OutputText.Valid || records[1].OutputText.String != "VERDICT: REWORK\n\nLatest validator failure." {
		t.Fatalf("expected latest validator output text, got %#v", records[1].OutputText)
	}
	if !records[1].OutcomeReasonCode.Valid || records[1].OutcomeReasonCode.String != "validation_gate_rework" {
		t.Fatalf("expected latest validator reason code, got %#v", records[1].OutcomeReasonCode)
	}
}

func TestStoreAcquireLeaseHonorsExpiry(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID: attemptID,
		RunID:     "run_1",
		NodeRunID: "nr_1",
		CreatedAt: "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}

	acquired, err := s.AcquireLease(ctx, AcquireLeaseRequest{
		AttemptID:    attemptID,
		LeaseOwner:   "worker_a",
		LeaseUntil:   "2026-03-01T12:01:00Z",
		AcquiredAt:   "2026-03-01T12:00:10Z",
		StateOnLease: "running",
	})
	if err != nil {
		t.Fatalf("acquire lease worker_a: %v", err)
	}
	if !acquired {
		t.Fatal("expected first lease acquire to succeed")
	}

	acquired, err = s.AcquireLease(ctx, AcquireLeaseRequest{
		AttemptID:    attemptID,
		LeaseOwner:   "worker_b",
		LeaseUntil:   "2026-03-01T12:02:00Z",
		AcquiredAt:   "2026-03-01T12:00:30Z",
		StateOnLease: "running",
	})
	if err != nil {
		t.Fatalf("acquire lease worker_b before expiry: %v", err)
	}
	if acquired {
		t.Fatal("expected competing lease acquire before expiry to fail")
	}

	acquired, err = s.AcquireLease(ctx, AcquireLeaseRequest{
		AttemptID:    attemptID,
		LeaseOwner:   "worker_b",
		LeaseUntil:   "2026-03-01T12:03:00Z",
		AcquiredAt:   "2026-03-01T12:01:01Z",
		StateOnLease: "running",
	})
	if err != nil {
		t.Fatalf("acquire lease worker_b after expiry: %v", err)
	}
	if !acquired {
		t.Fatal("expected lease steal after expiry to succeed")
	}
}

func TestStoreCommitTerminalWritesOutboxAndRejectsSecondTerminal(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID: attemptID,
		RunID:     "run_1",
		NodeRunID: "nr_1",
		CreatedAt: "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}

	if err := s.CommitTerminal(ctx, CommitTerminalRequest{
		OutboxID:          "outbox_terminal_1",
		AttemptID:         attemptID,
		RunID:             "run_1",
		NodeRunID:         "nr_1",
		TerminalSeq:       2,
		TerminalState:     "committed_success",
		PayloadJSON:       `{"status":"ok"}`,
		CommittedAt:       "2026-03-01T12:00:30Z",
		ProviderSessionID: "ses_terminal_1",
	}); err != nil {
		t.Fatalf("commit terminal first call: %v", err)
	}

	var state string
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&state); err != nil {
		t.Fatalf("query terminal state: %v", err)
	}
	if state != "committed_success" {
		t.Fatalf("expected committed_success terminal state, got %q", state)
	}

	var outboxCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&outboxCount); err != nil {
		t.Fatalf("query outbox count: %v", err)
	}
	if outboxCount != 1 {
		t.Fatalf("expected one outbox record, got %d", outboxCount)
	}

	var terminalEventType string
	var terminalReasonCode string
	var terminalPayload string
	if err := db.QueryRow(`SELECT event_type, reason_code, payload_json FROM attempt_event_log WHERE attempt_id = ? AND seq = 2`, attemptID).Scan(&terminalEventType, &terminalReasonCode, &terminalPayload); err != nil {
		t.Fatalf("query terminal attempt event: %v", err)
	}
	if terminalEventType != "attempt.succeeded" {
		t.Fatalf("expected terminal attempt event_type attempt.succeeded, got %q", terminalEventType)
	}
	if terminalReasonCode != "execution_completed" {
		t.Fatalf("expected terminal attempt reason_code execution_completed, got %q", terminalReasonCode)
	}
	if terminalPayload != `{"status":"ok"}` {
		t.Fatalf("expected terminal attempt payload to match commit payload, got %q", terminalPayload)
	}

	var providerSessionID string
	if err := db.QueryRow(`SELECT provider_session_id FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&providerSessionID); err != nil {
		t.Fatalf("query persisted provider_session_id: %v", err)
	}
	if providerSessionID != "ses_terminal_1" {
		t.Fatalf("expected persisted provider_session_id ses_terminal_1, got %q", providerSessionID)
	}

	err := s.CommitTerminal(ctx, CommitTerminalRequest{
		OutboxID:      "outbox_terminal_2",
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		TerminalSeq:   3,
		TerminalState: "committed_failure",
		PayloadJSON:   `{"status":"error"}`,
		CommittedAt:   "2026-03-01T12:00:31Z",
	})
	if err == nil {
		t.Fatal("expected second terminal commit attempt to fail")
	}
}

func TestStoreNextAttemptSeqAndQueuedListing(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID: attemptID,
		RunID:     "run_1",
		NodeRunID: "nr_1",
		CreatedAt: "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}

	queued, err := s.ListQueuedAttempts(ctx, 10)
	if err != nil {
		t.Fatalf("list queued attempts: %v", err)
	}
	if len(queued) != 1 || queued[0].AttemptID != attemptID {
		t.Fatalf("expected one queued attempt %q, got %#v", attemptID, queued)
	}

	seq, err := s.NextAttemptSeq(ctx, attemptID)
	if err != nil {
		t.Fatalf("next attempt seq (empty): %v", err)
	}
	if seq != 1 {
		t.Fatalf("expected next seq 1 for empty event log, got %d", seq)
	}

	if err := s.AppendAttemptEvent(ctx, AppendAttemptEventRequest{
		AttemptEventID: "ael_1",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		Seq:            1,
		EventType:      "attempt.started",
		ReasonCode:     "orchestration_progress",
		PayloadJSON:    `{}`,
		CreatedAt:      "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("append attempt event: %v", err)
	}

	seq, err = s.NextAttemptSeq(ctx, attemptID)
	if err != nil {
		t.Fatalf("next attempt seq after append: %v", err)
	}
	if seq != 2 {
		t.Fatalf("expected next seq 2 after one event, got %d", seq)
	}
}

func TestStoreListPendingOutboxAndPublish(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID: attemptID,
		RunID:     "run_1",
		NodeRunID: "nr_1",
		CreatedAt: "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := s.CommitTerminal(ctx, CommitTerminalRequest{
		OutboxID:      "outbox_terminal_1",
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		TerminalSeq:   2,
		TerminalState: "committed_success",
		PayloadJSON:   `{"status":"ok"}`,
		CommittedAt:   "2026-03-01T12:00:30Z",
	}); err != nil {
		t.Fatalf("commit terminal: %v", err)
	}

	pending, err := s.ListPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("list pending outbox: %v", err)
	}
	if len(pending) != 1 || pending[0].OutboxID != "outbox_terminal_1" {
		t.Fatalf("expected pending outbox record, got %#v", pending)
	}

	if err := s.MarkOutboxPublished(ctx, "outbox_terminal_1", "2026-03-01T12:00:40Z"); err != nil {
		t.Fatalf("mark outbox published: %v", err)
	}

	pending, err = s.ListPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("list pending outbox after publish: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending outbox records after publish, got %#v", pending)
	}
}

func TestStoreRecordOutboxFailureAndMarkFailed(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID: attemptID,
		RunID:     "run_1",
		NodeRunID: "nr_1",
		CreatedAt: "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := s.CommitTerminal(ctx, CommitTerminalRequest{
		OutboxID:      "outbox_terminal_1",
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		TerminalSeq:   2,
		TerminalState: "committed_success",
		PayloadJSON:   `{"status":"ok"}`,
		CommittedAt:   "2026-03-01T12:00:30Z",
	}); err != nil {
		t.Fatalf("commit terminal: %v", err)
	}

	if err := s.RecordOutboxFailure(ctx, "outbox_terminal_1", "projection failed", "2026-03-01T12:00:35Z"); err != nil {
		t.Fatalf("record outbox failure: %v", err)
	}

	var status string
	var publishAttempts int
	var lastError string
	if err := db.QueryRow(`SELECT status, publish_attempts, last_error FROM attempt_outbox WHERE outbox_id = 'outbox_terminal_1'`).Scan(&status, &publishAttempts, &lastError); err != nil {
		t.Fatalf("query outbox failure bookkeeping: %v", err)
	}
	if status != "pending" {
		t.Fatalf("expected outbox to remain pending after retryable failure, got %q", status)
	}
	if publishAttempts != 1 {
		t.Fatalf("expected publish_attempts=1 after retryable failure, got %d", publishAttempts)
	}
	if lastError != "projection failed" {
		t.Fatalf("expected last_error to be recorded, got %q", lastError)
	}

	if err := s.MarkOutboxFailed(ctx, "outbox_terminal_1", "malformed replay payload", "2026-03-01T12:00:36Z"); err != nil {
		t.Fatalf("mark outbox failed: %v", err)
	}
	if err := db.QueryRow(`SELECT status, publish_attempts, last_error FROM attempt_outbox WHERE outbox_id = 'outbox_terminal_1'`).Scan(&status, &publishAttempts, &lastError); err != nil {
		t.Fatalf("query outbox failed status: %v", err)
	}
	if status != "failed" {
		t.Fatalf("expected outbox failed status, got %q", status)
	}
	if publishAttempts != 2 {
		t.Fatalf("expected publish_attempts=2 after dead-lettering, got %d", publishAttempts)
	}
	if lastError != "malformed replay payload" {
		t.Fatalf("expected dead-letter last_error to be recorded, got %q", lastError)
	}

	pending, err := s.ListPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("list pending outbox after dead-lettering: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected failed outbox rows to be omitted from pending list, got %#v", pending)
	}
}

func TestStoreQueueAttemptCancelIsIdempotent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID: attemptID,
		RunID:     "run_1",
		NodeRunID: "nr_1",
		CreatedAt: "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}

	req := QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_outbox_1",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionID:      "session_run_1_decree_intake_1",
		ReasonCode:     "run_cancelled_by_user",
		ReasonText:     "operator cancelled the run",
		RequestedAt:    "2026-03-01T12:00:02Z",
		CreatedAt:      "2026-03-01T12:00:02Z",
	}
	if err := s.QueueAttemptCancel(ctx, req); err != nil {
		t.Fatalf("queue attempt cancel first insert: %v", err)
	}
	if err := s.QueueAttemptCancel(ctx, req); err != nil {
		t.Fatalf("queue attempt cancel second insert: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_cancel_outbox WHERE attempt_id = ?`, attemptID).Scan(&count); err != nil {
		t.Fatalf("query attempt cancel outbox rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one attempt cancel outbox row, got %d", count)
	}
}

func TestStoreListPendingAttemptCancelsPublishAndFailure(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID: attemptID,
		RunID:     "run_1",
		NodeRunID: "nr_1",
		CreatedAt: "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := s.QueueAttemptCancel(ctx, QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_outbox_1",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionID:      "session_run_1_decree_intake_1",
		ReasonCode:     "run_terminalized",
		ReasonText:     "run moved to failed",
		RequestedAt:    "2026-03-01T12:00:02Z",
		CreatedAt:      "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("queue attempt cancel: %v", err)
	}

	pending, err := s.ListPendingAttemptCancels(ctx, 10)
	if err != nil {
		t.Fatalf("list pending attempt cancels: %v", err)
	}
	if len(pending) != 1 || pending[0].CancelOutboxID != "cancel_outbox_1" {
		t.Fatalf("expected one pending attempt cancel row, got %#v", pending)
	}
	if pending[0].SessionID != "session_run_1_decree_intake_1" {
		t.Fatalf("expected logical fallback session id, got %q", pending[0].SessionID)
	}

	if err := s.RecordAttemptCancelFailure(ctx, "cancel_outbox_1", "transport timeout", "2026-03-01T12:00:03Z"); err != nil {
		t.Fatalf("record attempt cancel failure: %v", err)
	}

	var publishAttempts int
	var lastError string
	if err := db.QueryRow(`SELECT publish_attempts, last_error FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel_outbox_1'`).Scan(&publishAttempts, &lastError); err != nil {
		t.Fatalf("query attempt cancel failure bookkeeping: %v", err)
	}
	if publishAttempts != 1 {
		t.Fatalf("expected publish_attempts=1 after failure, got %d", publishAttempts)
	}
	if lastError != "transport timeout" {
		t.Fatalf("expected last_error to be recorded, got %q", lastError)
	}

	if err := s.MarkAttemptCancelPublished(ctx, "cancel_outbox_1", "2026-03-01T12:00:04Z"); err != nil {
		t.Fatalf("mark attempt cancel published: %v", err)
	}

	pending, err = s.ListPendingAttemptCancels(ctx, 10)
	if err != nil {
		t.Fatalf("list pending attempt cancels after publish: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending attempt cancels after publish, got %#v", pending)
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel_outbox_1'`).Scan(&status); err != nil {
		t.Fatalf("query attempt cancel status: %v", err)
	}
	if status != "published" {
		t.Fatalf("expected published attempt cancel status, got %q", status)
	}
}

func TestStoreListPendingAttemptCancelsPrefersProviderSessionID(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:  attemptID,
		RunID:      "run_1",
		NodeRunID:  "nr_1",
		SessionRef: "session_run_1_decree_intake_1",
		CreatedAt:  "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := s.UpdateProviderSessionID(ctx, UpdateProviderSessionIDRequest{
		AttemptID:         attemptID,
		ProviderSessionID: "ses_real_456",
		UpdatedAt:         "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("update provider session id: %v", err)
	}
	if err := s.QueueAttemptCancel(ctx, QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_outbox_1",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionID:      "session_run_1_decree_intake_1",
		ReasonCode:     "run_terminalized",
		ReasonText:     "run moved to failed",
		RequestedAt:    "2026-03-01T12:00:02Z",
		CreatedAt:      "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("queue attempt cancel: %v", err)
	}

	pending, err := s.ListPendingAttemptCancels(ctx, 10)
	if err != nil {
		t.Fatalf("list pending attempt cancels: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected one pending cancel, got %#v", pending)
	}
	if pending[0].SessionID != "ses_real_456" {
		t.Fatalf("expected provider session id preference, got %q", pending[0].SessionID)
	}
}

func TestStoreQueueOutboxIsIdempotent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID: attemptID,
		RunID:     "run_1",
		NodeRunID: "nr_1",
		CreatedAt: "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}

	req := QueueOutboxRequest{
		OutboxID:      "outbox_queue_1",
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		TerminalSeq:   1,
		TerminalState: "committed_success",
		PayloadJSON:   `{"phase":"exec_done"}`,
		CreatedAt:     "2026-03-01T12:00:02Z",
	}
	if err := s.QueueOutbox(ctx, req); err != nil {
		t.Fatalf("queue outbox first insert: %v", err)
	}
	if err := s.QueueOutbox(ctx, req); err != nil {
		t.Fatalf("queue outbox second insert: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_outbox WHERE attempt_id = ? AND terminal_seq = 1`, attemptID).Scan(&count); err != nil {
		t.Fatalf("query queued outbox rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one queued outbox row, got %d", count)
	}
}

func TestStoreFinalizeTerminalWritesSettledAttemptStateAndUpsertsLegacyEvent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID: attemptID,
		RunID:     "run_1",
		NodeRunID: "nr_1",
		CreatedAt: "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := s.QueueOutbox(ctx, QueueOutboxRequest{
		OutboxID:      "outbox_terminal_finalize",
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		TerminalSeq:   2,
		TerminalState: "committed_success",
		PayloadJSON:   `{"status":"raw"}`,
		CreatedAt:     "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("queue outbox: %v", err)
	}
	if err := s.AppendAttemptEvent(ctx, AppendAttemptEventRequest{
		AttemptEventID: "attempt_event:run_1:decree_intake:1:2",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		Seq:            2,
		EventType:      "attempt.succeeded",
		ReasonCode:     "execution_completed",
		PayloadJSON:    `{"status":"raw"}`,
		CreatedAt:      "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("append legacy terminal event: %v", err)
	}

	result, err := s.FinalizeTerminal(ctx, FinalizeTerminalRequest{
		OutboxID:          "outbox_terminal_finalize",
		AttemptID:         attemptID,
		RunID:             "run_1",
		NodeRunID:         "nr_1",
		TerminalState:     "committed_failure",
		PayloadJSON:       `{"error":"settled failure"}`,
		CommittedAt:       "2026-03-01T12:00:30Z",
		FailureCode:       "provider_empty_output",
		FailureText:       "settled failure",
		ProviderSessionID: "ses_terminal_2",
	})
	if err != nil {
		t.Fatalf("finalize terminal: %v", err)
	}
	if !result.Applied || result.AttemptState != "committed_failure" {
		t.Fatalf("expected finalized terminal result, got %#v", result)
	}

	var state, failureCode, failureText, providerSessionID string
	if err := db.QueryRow(`SELECT state, failure_code, failure_text, provider_session_id FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&state, &failureCode, &failureText, &providerSessionID); err != nil {
		t.Fatalf("query finalized attempt: %v", err)
	}
	if state != "committed_failure" {
		t.Fatalf("expected committed_failure finalized state, got %q", state)
	}
	if failureCode != "provider_empty_output" || failureText != "settled failure" {
		t.Fatalf("unexpected finalized failure metadata code=%q text=%q", failureCode, failureText)
	}
	if providerSessionID != "ses_terminal_2" {
		t.Fatalf("expected finalized provider session id ses_terminal_2, got %q", providerSessionID)
	}

	var eventType, reasonCode, payload string
	if err := db.QueryRow(`SELECT event_type, reason_code, payload_json FROM attempt_event_log WHERE attempt_id = ? AND seq = 2`, attemptID).Scan(&eventType, &reasonCode, &payload); err != nil {
		t.Fatalf("query finalized terminal event: %v", err)
	}
	if eventType != "attempt.failed" || reasonCode != "provider_empty_output" || payload != `{"error":"settled failure"}` {
		t.Fatalf("unexpected finalized terminal event type=%q reason=%q payload=%q", eventType, reasonCode, payload)
	}
}

func TestStoreFinalizeTerminalNoopsWhenAttemptAlreadyTerminalized(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID: attemptID,
		RunID:     "run_1",
		NodeRunID: "nr_1",
		CreatedAt: "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	mustExec(t, db, `UPDATE attempt_executions
		SET state = 'committed_cancelled',
		    terminal_observed_at = '2026-03-01T12:00:05Z',
		    committed_at = '2026-03-01T12:00:05Z',
		    failure_code = 'run_terminalized',
		    failure_text = 'run cancelled before completion',
		    updated_at = '2026-03-01T12:00:05Z'
		WHERE attempt_id = 'run_1:decree_intake:1'`)
	if err := s.QueueOutbox(ctx, QueueOutboxRequest{
		OutboxID:      "outbox_terminal_noop",
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		TerminalSeq:   2,
		TerminalState: "committed_success",
		PayloadJSON:   `{"status":"late-success"}`,
		CreatedAt:     "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("queue outbox: %v", err)
	}

	result, err := s.FinalizeTerminal(ctx, FinalizeTerminalRequest{
		OutboxID:          "outbox_terminal_noop",
		AttemptID:         attemptID,
		RunID:             "run_1",
		NodeRunID:         "nr_1",
		TerminalState:     "committed_success",
		PayloadJSON:       `{"status":"late-success"}`,
		CommittedAt:       "2026-03-01T12:00:30Z",
		ProviderSessionID: "ses_late_success",
	})
	if err != nil {
		t.Fatalf("finalize terminal noop: %v", err)
	}
	if result.Applied {
		t.Fatalf("expected finalized terminal noop result, got %#v", result)
	}
	if result.AttemptState != "committed_cancelled" {
		t.Fatalf("expected finalized terminal noop to preserve committed_cancelled, got %#v", result)
	}

	var state, failureCode, providerSessionID string
	if err := db.QueryRow(`SELECT state, failure_code, provider_session_id FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&state, &failureCode, &providerSessionID); err != nil {
		t.Fatalf("query finalized noop attempt: %v", err)
	}
	if state != "committed_cancelled" || failureCode != "run_terminalized" {
		t.Fatalf("expected cancelled attempt state to remain canonical, got state=%q failure_code=%q", state, failureCode)
	}
	if providerSessionID != "" {
		t.Fatalf("expected noop finalization not to rewrite provider session id, got %q", providerSessionID)
	}

	var terminalEventCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_event_log WHERE attempt_id = ? AND seq = 2`, attemptID).Scan(&terminalEventCount); err != nil {
		t.Fatalf("count terminal noop attempt events: %v", err)
	}
	if terminalEventCount != 0 {
		t.Fatalf("expected noop finalization not to rewrite terminal attempt events, got %d", terminalEventCount)
	}

	var outboxTerminalState string
	if err := db.QueryRow(`SELECT terminal_state FROM attempt_outbox WHERE outbox_id = 'outbox_terminal_noop'`).Scan(&outboxTerminalState); err != nil {
		t.Fatalf("query terminal noop outbox state: %v", err)
	}
	if outboxTerminalState != "committed_cancelled" {
		t.Fatalf("expected noop finalization to preserve canonical outbox terminal state, got %q", outboxTerminalState)
	}
}

func TestStoreFinalizeTerminalPreservesSuccessReasonCodeInAttemptEventLog(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID: attemptID,
		RunID:     "run_1",
		NodeRunID: "nr_1",
		CreatedAt: "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := s.QueueOutbox(ctx, QueueOutboxRequest{
		OutboxID:      "outbox_terminal_success_reason",
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		TerminalSeq:   2,
		TerminalState: "committed_success",
		PayloadJSON:   `{"attempt_id":"run_1:decree_intake:1"}`,
		CreatedAt:     "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("queue outbox: %v", err)
	}

	result, err := s.FinalizeTerminal(ctx, FinalizeTerminalRequest{
		OutboxID:           "outbox_terminal_success_reason",
		AttemptID:          attemptID,
		RunID:              "run_1",
		NodeRunID:          "nr_1",
		TerminalState:      "committed_success",
		PayloadJSON:        `{"status":"ok"}`,
		CommittedAt:        "2026-03-01T12:00:30Z",
		TerminalReasonCode: "execution_review_cap_auto_succeeded",
	})
	if err != nil {
		t.Fatalf("finalize terminal success reason: %v", err)
	}
	if !result.Applied || result.AttemptState != "committed_success" {
		t.Fatalf("expected committed_success finalize result, got %#v", result)
	}

	var state, failureCode string
	if err := db.QueryRow(`SELECT state, failure_code FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&state, &failureCode); err != nil {
		t.Fatalf("query finalized success attempt: %v", err)
	}
	if state != "committed_success" {
		t.Fatalf("expected committed_success state, got %q", state)
	}
	if failureCode != "" {
		t.Fatalf("expected success finalize to keep failure_code blank, got %q", failureCode)
	}

	var eventType, reasonCode string
	if err := db.QueryRow(`SELECT event_type, reason_code FROM attempt_event_log WHERE attempt_id = ? AND seq = 2`, attemptID).Scan(&eventType, &reasonCode); err != nil {
		t.Fatalf("query finalized success attempt event: %v", err)
	}
	if eventType != "attempt.succeeded" {
		t.Fatalf("expected attempt.succeeded event type, got %q", eventType)
	}
	if reasonCode != "execution_review_cap_auto_succeeded" {
		t.Fatalf("expected success reason override to be preserved, got %q", reasonCode)
	}
}

func seedCanonicalProviderExecutionOutput(t *testing.T, db *sql.DB, attemptID, runID, nodeRunID, stateValue, failureCode, failureText, outputText, createdAt string) {
	t.Helper()
	s := New(db)
	providerInvocationID := fmt.Sprintf("pinv_%s", strings.NewReplacer(":", "_", "-", "_").Replace(attemptID))
	providerSessionID := fmt.Sprintf("ses_%s", strings.NewReplacer(":", "_", "-", "_").Replace(attemptID))
	completionCapability := fmt.Sprintf("cap_%s", strings.NewReplacer(":", "_", "-", "_").Replace(attemptID))
	if err := s.EnqueueAttempt(context.Background(), EnqueueAttemptRequest{
		AttemptID:         attemptID,
		RunID:             runID,
		NodeRunID:         nodeRunID,
		ExecutionMode:     ExecutionModeProviderBacked,
		SessionRef:        "session_" + strings.NewReplacer(":", "_", "-", "_").Replace(attemptID),
		ProviderSessionID: providerSessionID,
		CreatedAt:         createdAt,
	}); err != nil {
		t.Fatalf("enqueue canonical provider attempt %q: %v", attemptID, err)
	}
	if _, err := s.CreateProviderInvocation(context.Background(), CreateProviderInvocationRequest{
		ProviderInvocationID: providerInvocationID,
		AttemptID:            attemptID,
		RunID:                runID,
		NodeRunID:            nodeRunID,
		LogicalSessionID:     "session_" + strings.NewReplacer(":", "_", "-", "_").Replace(attemptID),
		ProviderSessionID:    providerSessionID,
		CompletionCapability: completionCapability,
		IssuedAt:             createdAt,
		CreatedAt:            createdAt,
	}); err != nil {
		t.Fatalf("create canonical provider invocation %q: %v", attemptID, err)
	}
	if _, err := s.MarkProviderInvocationOpen(context.Background(), MarkProviderInvocationOpenRequest{
		ProviderInvocationID: providerInvocationID,
		AttemptID:            attemptID,
		ProviderSessionID:    providerSessionID,
		OpenedAt:             createdAt,
	}); err != nil {
		t.Fatalf("mark canonical provider invocation open %q: %v", attemptID, err)
	}
	structuredPayloadJSON := canonicalStructuredPayloadForStoreTest(outputText)
	primaryEvidenceID := fmt.Sprintf("evidence_terminal_%s", strings.NewReplacer(":", "_", "-", "_").Replace(attemptID))
	if strings.TrimSpace(outputText) != "" {
		primaryEvidenceID = fmt.Sprintf("receipt_%s", strings.NewReplacer(":", "_", "-", "_").Replace(attemptID))
		if _, err := s.AcceptProviderCompletionReceipt(context.Background(), AcceptProviderCompletionReceiptRequest{
			EvidenceID:            primaryEvidenceID,
			ProviderInvocationID:  providerInvocationID,
			AttemptID:             attemptID,
			RunID:                 runID,
			NodeRunID:             nodeRunID,
			ProtocolVersion:       "2026-05-01",
			CompletionCapability:  completionCapability,
			FinalText:             outputText,
			StructuredPayloadJSON: structuredPayloadJSON,
			ReceivedAt:            createdAt,
		}); err != nil {
			t.Fatalf("accept canonical provider receipt %q: %v", attemptID, err)
		}
	} else {
		if _, err := s.AppendProviderCompletionEvidence(context.Background(), AppendProviderCompletionEvidenceRequest{
			EvidenceID:           primaryEvidenceID,
			ProviderInvocationID: providerInvocationID,
			AttemptID:            attemptID,
			RunID:                runID,
			NodeRunID:            nodeRunID,
			EvidenceKind:         ProviderCompletionEvidenceKindStreamObservation,
			PayloadJSON:          `{"diagnostic":"missing canonical response"}`,
			CreatedAt:            createdAt,
		}); err != nil {
			t.Fatalf("append canonical provider evidence %q: %v", attemptID, err)
		}
	}
	settlementOutcome := ProviderCompletionOutcomeSuccess
	switch strings.TrimSpace(stateValue) {
	case "committed_failure":
		settlementOutcome = ProviderCompletionOutcomeFailed
	case "committed_cancelled":
		settlementOutcome = ProviderCompletionOutcomeCancelled
	}
	if _, err := s.FinalizeProviderCompletionSettlement(context.Background(), FinalizeProviderCompletionSettlementRequest{
		SettlementID:          fmt.Sprintf("settlement_%s", strings.NewReplacer(":", "_", "-", "_").Replace(attemptID)),
		ProviderInvocationID:  providerInvocationID,
		AttemptID:             attemptID,
		RunID:                 runID,
		NodeRunID:             nodeRunID,
		Outcome:               settlementOutcome,
		PrimaryEvidenceID:     primaryEvidenceID,
		FinalText:             outputText,
		StructuredPayloadJSON: structuredPayloadJSON,
		FailureReasonCode:     failureCode,
		FailureReasonText:     failureText,
		SettledAt:             createdAt,
	}); err != nil {
		t.Fatalf("finalize canonical provider settlement %q: %v", attemptID, err)
	}
}

func canonicalStructuredPayloadForStoreTest(outputText string) string {
	for _, line := range strings.Split(strings.TrimSpace(outputText), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		switch trimmed {
		case "VERDICT: PASS":
			return `{"verdict":"PASS"}`
		case "VERDICT: REWORK":
			return `{"verdict":"REWORK"}`
		default:
			return `{}`
		}
	}
	return `{}`
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite test db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}

	return db
}

func applyMigration(t *testing.T, db *sql.DB, fileName string) {
	t.Helper()
	_ = fileName

	entries, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	for _, name := range entries {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		if _, err := db.Exec(string(raw)); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}
}

func seedMinimumGraph(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_1', 'D1', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_1', 'royal-decree', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_1', 'wfdef_1', 'decree_intake', 'king', 'decree_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_2', 'wfdef_1', 'royal_closure', 'king', 'royal_closure', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_1', 'wfdef_1', 'decree_intake', 'royal_closure', 'always', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_1', NULL, 'decree_1', 'wfdef_1', 'running', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_1', 'run_1', 'decree_intake', 1,1, 'running', 'king', 'agent_king', 1, NULL, NULL)`)
}

func mustExec(t *testing.T, db *sql.DB, statement string) {
	t.Helper()
	if _, err := db.Exec(statement); err != nil {
		t.Fatalf("exec failed for %q: %v", statement, err)
	}
}
